//! The canonical wire contract is api/ssf/v1/node.proto.
pub mod v1 { tonic::include_proto!("ssf.v1"); }
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
pub async fn connect(uri: String, domain: String, ca: Vec<u8>, cert: Vec<u8>, key: Vec<u8>)
    -> Result<v1::runtime_storage_node_client::RuntimeStorageNodeClient<Channel>, Box<dyn std::error::Error>> {
    let tls = ClientTlsConfig::new().domain_name(domain)
        .ca_certificate(Certificate::from_pem(ca)).identity(Identity::from_pem(cert, key));
    let channel = Endpoint::from_shared(uri)?.tls_config(tls)?.connect().await?;
    Ok(v1::runtime_storage_node_client::RuntimeStorageNodeClient::new(channel))
}
