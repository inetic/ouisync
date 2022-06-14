use futures_util::StreamExt;
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const CERT_DOMAIN: &str = "ouisync.net";

//------------------------------------------------------------------------------
pub struct Connector {
    endpoint: quinn::Endpoint,
}

impl Connector {
    pub async fn connect(&self, remote_addr: SocketAddr) -> Result<Connection, Error> {
        let quinn::NewConnection { connection, .. } =
            self.endpoint.connect(remote_addr, CERT_DOMAIN)?.await?;
        let (tx, rx) = connection.open_bi().await?;
        Ok(Connection::new(connection, rx, tx))
    }
}

//------------------------------------------------------------------------------
pub struct Acceptor {
    incoming: quinn::Incoming,
    local_addr: SocketAddr,
}

impl Acceptor {
    pub async fn accept(&mut self) -> Result<Connection, Error> {
        let incoming_conn = match self.incoming.next().await {
            Some(incoming_conn) => incoming_conn,
            None => return Err(Error::DoneAccepting),
        };
        let quinn::NewConnection {
            connection,
            mut bi_streams,
            ..
        } = incoming_conn.await?;
        let (tx, rx) = match bi_streams.next().await {
            Some(r) => r?,
            None => return Err(Error::DoneAccepting),
        };
        Ok(Connection::new(connection, rx, tx))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

//------------------------------------------------------------------------------
pub struct Connection {
    connection: quinn::Connection,
    rx: quinn::RecvStream,
    // It's `Option` because we `take` out of it in `Drop` and/or `finish`.
    tx: Option<quinn::SendStream>,
}

impl Connection {
    pub fn new(
        connection: quinn::Connection,
        rx: quinn::RecvStream,
        tx: quinn::SendStream,
    ) -> Self {
        Self {
            connection,
            rx,
            tx: Some(tx),
        }
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.connection.remote_address()
    }

    /// Make sure all data is sent, no more data can be sent afterwards.
    pub async fn finish(&mut self) -> Result<(), Error> {
        match self.tx.take() {
            Some(mut tx) => {
                tx.finish().await?;
                Ok(())
            }
            None => Err(Error::Write(quinn::WriteError::UnknownStream)),
        }
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().rx).poll_read(cx, buf)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().tx {
            Some(tx) => Pin::new(tx).poll_write(cx, buf),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "already finished",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match &mut self.get_mut().tx {
            Some(tx) => Pin::new(tx).poll_flush(cx),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "already finished",
            ))),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match &mut self.get_mut().tx {
            Some(tx) => Pin::new(tx).poll_shutdown(cx),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "already finished",
            ))),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(mut tx) = self.tx.take() {
            tokio::task::spawn(async move { tx.finish().await.unwrap_or(()) });
        }
    }
}

//------------------------------------------------------------------------------
pub fn configure(addr: SocketAddr) -> Result<(Connector, Acceptor), Error> {
    let server_config = make_server_config()?;
    let (mut endpoint, incoming) = quinn::Endpoint::server(server_config, addr)?;
    endpoint.set_default_client_config(make_client_config());

    let local_addr = endpoint.local_addr()?;

    let connector = Connector { endpoint };
    let acceptor = Acceptor {
        incoming,
        local_addr,
    };

    Ok((connector, acceptor))
}

//------------------------------------------------------------------------------
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connect error")]
    Connect,
    #[error("connection error")]
    Connection,
    #[error("write error")]
    Write(quinn::WriteError),
    #[error("done accepting error")]
    DoneAccepting,
    #[error("IO error")]
    Io(std::io::Error),
    #[error("TLS error")]
    Tls(rustls::Error),
}

impl From<quinn::ConnectionError> for Error {
    fn from(_: quinn::ConnectionError) -> Self {
        Self::Connection
    }
}

impl From<quinn::ConnectError> for Error {
    fn from(_: quinn::ConnectError) -> Self {
        Self::Connect
    }
}

impl From<quinn::WriteError> for Error {
    fn from(e: quinn::WriteError) -> Self {
        Self::Write(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<rustls::Error> for Error {
    fn from(e: rustls::Error) -> Self {
        Self::Tls(e)
    }
}

//------------------------------------------------------------------------------
// Dummy certificate verifier that treats any certificate as valid. In our P2P system there are no
// certification authorities, which makes the whole verification and encryption a dead weight.
struct SkipServerVerification;

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

fn make_client_config() -> quinn::ClientConfig {
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification {}))
        .with_no_client_auth();

    quinn::ClientConfig::new(Arc::new(crypto))
}

fn make_server_config() -> Result<quinn::ServerConfig, Error> {
    // Generate self signed certificate, it won't be checked, but QUIC doesn't have an option to
    // get rid of TLS completely.
    let cert = rcgen::generate_simple_self_signed(vec![CERT_DOMAIN.into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = cert.serialize_private_key_der();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];

    let mut server_config = quinn::ServerConfig::with_single_cert(cert_chain, priv_key)?;

    // We'll be using bi-directional streams only.
    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .max_concurrent_uni_streams(0_u8.into());

    Ok(server_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        task,
    };

    #[tokio::test]
    async fn small_data_exchange() {
        let (connector, mut acceptor) = configure("127.0.0.1:0".parse().unwrap()).unwrap();

        let addr = acceptor.local_addr();

        let message = b"hello world";

        let h1 = task::spawn(async move {
            let mut conn = acceptor.accept().await.unwrap();
            let mut buf = [0; 32];
            let n = conn.read(&mut buf).await.unwrap();
            assert_eq!(message, &buf[..n]);
        });

        let h2 = task::spawn(async move {
            let mut conn = connector.connect(addr).await.unwrap();
            conn.write_all(message).await.unwrap();
            conn.finish().await.unwrap();
        });

        h1.await.unwrap();
        h2.await.unwrap();
    }
}
