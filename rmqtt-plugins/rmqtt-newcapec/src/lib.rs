#![deny(unsafe_code)]

use rmqtt::{async_trait::async_trait, log};

use rmqtt::{
    broker::hook::{Handler, HookResult, Parameter, Register, ReturnType, Type},
    plugin::{DynPlugin, DynPluginResult, Plugin},
    Result, Runtime,
};

#[inline]
pub async fn register(
    runtime: &'static Runtime,
    name: &'static str,
    descr: &'static str,
    default_startup: bool,
    immutable: bool,
) -> Result<()> {
    runtime
        .plugins
        .register(name, default_startup, immutable, move || -> DynPluginResult {
            Box::pin(async move {
                NewcapecPlugin::new(runtime, name, descr).await.map(|p| -> DynPlugin { Box::new(p) })
            })
        })
        .await?;
    Ok(())
}

struct NewcapecPlugin {
    name: String,
    descr: String,
    register: Box<dyn Register>,
}

impl NewcapecPlugin {
    async fn new<N: Into<String>, D: Into<String>>(
        runtime: &'static Runtime,
        name: N,
        descr: D,
    ) -> Result<Self> {
        let register = runtime.extends.hook_mgr().await.register();
        Ok(Self { name: name.into(), descr: descr.into(), register })
    }
}

#[async_trait]
impl Plugin for NewcapecPlugin {
    #[inline]
    async fn init(&mut self) -> Result<()> {
        log::info!("{} init", self.name);
        self.register.add_priority(Type::ClientConnack, 0, Box::new(NewcapecHandler::new())).await;
        self.register.add_priority(Type::ClientConnect, 0, Box::new(NewcapecHandler::new())).await;
        Ok(())
    }

    // #[inline]
    // fn name(&self) -> &str {
    //     &self.name
    // }

    // #[inline]
    // fn version(&self) -> &str {
    //     "0.1.1"
    // }

    // #[inline]
    // fn descr(&self) -> &str {
    //     &self.descr
    // }

    #[inline]
    async fn start(&mut self) -> Result<()> {
        log::info!("{} start", self.name);
        self.register.start().await;
        Ok(())
    }

    #[inline]
    async fn stop(&mut self) -> Result<bool> {
        log::info!("{} stop", self.name);
        self.register.stop().await;
        Ok(true)
    }
}

struct NewcapecHandler;

impl NewcapecHandler {
    #[inline]
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Handler for NewcapecHandler {
    async fn hook(&self, param: &Parameter, acc: Option<HookResult>) -> ReturnType {
        log::info!("NewcapecHandler hook");
        let ok = match param {
            Parameter::ClientConnect(conn_info) => {
                log::info!("ClientConnect: {:?}", conn_info);
                true
            }
            Parameter::ClientConnack(conn_info, reason) => {
                log::info!("ClientConnack: {:?}, {:?}", conn_info, reason);
                true
            }
            _ => false,
        };
        (ok, acc)
    }
}
