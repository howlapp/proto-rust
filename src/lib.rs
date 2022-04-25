//! # furink-proto
//! Rust definitions for the fur.ink gRPC backend services.

//! Protocol version string, compiled from git revision and semver.
pub static VERSION: &'static str = env!("VERGEN_GIT_SEMVER");

pub mod discovery {
    //! Discoery service definitions.
    tonic::include_proto!("howl.discovery");
}

pub mod version {
    //! Version service definitions.
    tonic::include_proto!("howl.version");

    use std::{error::Error, time::Duration};

    use tonic::{
        transport::{Channel, Endpoint},
        Code, Request, Response, Status,
    };
    use tracing::debug;

    use crate::discovery::{
        discovery_service_client::DiscoveryServiceClient, HeartbeatPayload, RegisterRequest,
    };

    use self::{
        version_service_client::VersionServiceClient, version_service_server::VersionService,
    };
    use super::VERSION;

    /// A basic version verification service implementation.
    pub struct VersionServiceProvider;

    #[tonic::async_trait]
    impl VersionService for VersionServiceProvider {
        async fn validate(
            &self,
            request: Request<VersionRequest>,
        ) -> Result<Response<VersionResponse>, Status> {
            if request.get_ref().version == VERSION {
                Ok(Response::new(VersionResponse {
                    version: VERSION.to_string(),
                }))
            } else {
                Err(Status::new(Code::InvalidArgument, "Version mismatch"))
            }
        }
    }

    /// Validate and register a client with the service manager. Produces the service's assigned ID
    /// and a channel that can be used to send heartbeats to the service.
    pub async fn validate_and_register<S: Into<Endpoint>>(
        url: S,
        conf: RegisterRequest,
    ) -> Result<(String, Channel), Box<dyn Error>> {
        let endpoint: Endpoint = url.into();
        let channel = endpoint.connect().await?;
        // connect to version service and validate
        let mut version_client = VersionServiceClient::new(channel.clone());
        version_client
            .validate(VersionRequest {
                version: VERSION.to_string(),
            })
            .await?;
        // register with service manager
        let mut discovery_client = DiscoveryServiceClient::new(channel.clone());
        let res = discovery_client
            .register(Request::new(conf))
            .await?
            .into_inner();
        // consume and return channel
        Ok((res.id, channel))
    }

    /// The configuration used by the `spawn_heartbeat_task` function.
    pub struct HeartbeatConfig {
        /// The interval at which the heartbeat task should run.
        pub interval: Duration,
        /// The ID of the service heartbeating.
        pub id: String,
        /// The channel to use for heartbeating.
        pub channel: Channel,
    }

    /// Spawn an asynchronous task that will send heartbeats to the service manager.
    pub fn spawn_heartbeat_task(conf: HeartbeatConfig) {
        tokio::spawn(async move {
            let mut client = DiscoveryServiceClient::new(conf.channel);
            loop {
                debug!(message = "sending heartbeat", %conf.id);
                client
                    .heartbeat(Request::new(HeartbeatPayload {
                        id: conf.id.clone(),
                    }))
                    .await
                    .expect("failed to heartbeat server");
                tokio::time::sleep(conf.interval).await;
            }
        });
    }
}
