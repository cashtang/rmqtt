use bytestring::ByteString;
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use std::convert::From as _;
use std::convert::TryFrom;
use std::fmt;
use std::net::SocketAddr;
use std::num::{NonZeroU16, NonZeroU32};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::{MqttError, Result};

pub use ntex_mqtt::v3::{
    self, codec::Connect as ConnectV3, codec::ConnectAckReason as ConnectAckReasonV3,
    codec::LastWill as LastWillV3, codec::Packet as PacketV3,
    codec::SubscribeReturnCode as SubscribeReturnCodeV3, HandshakeAck as HandshakeAckV3,
    MqttSink as MqttSinkV3,
};

pub use ntex_mqtt::v5::{
    self, codec::Connect as ConnectV5, codec::ConnectAckReason as ConnectAckReasonV5,
    codec::LastWill as LastWillV5, codec::Packet as PacketV5, codec::Subscribe as SubscribeV5,
    codec::SubscribeAck as SubscribeAckV5, codec::SubscribeAckReason, codec::SubscriptionOptions,
    codec::Unsubscribe as UnsubscribeV5, codec::UnsubscribeAck as UnsubscribeAckV5,
    codec::UserProperties, codec::UserProperty, HandshakeAck as HandshakeAckV5,
    MqttSink as MqttSinkV5,
};

pub use ntex_mqtt::types::{MQTT_LEVEL_31, MQTT_LEVEL_311, MQTT_LEVEL_5};

pub type NodeId = u64;
pub type RemoteSocketAddr = SocketAddr;
pub type LocalSocketAddr = SocketAddr;
pub type ClientId = bytestring::ByteString;
pub type UserName = bytestring::ByteString;
pub type Password = bytes::Bytes;
pub type PacketId = u16;
pub type Reason = bytestring::ByteString;
pub type TopicName = bytestring::ByteString;
///topic name or topic filter
pub type Topic = ntex_mqtt::Topic;
///topic filter
pub type TopicFilter = Topic;
pub type Disconnect = bool;
pub type MessageExpiry = bool;
pub type TimestampMillis = i64;

pub type Tx = mpsc::UnboundedSender<Message>;
pub type Rx = mpsc::UnboundedReceiver<Message>;

pub type StdHashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;
pub type QoS = ntex_mqtt::types::QoS;
pub type PublishReceiveTime = TimestampMillis;
pub type TopicFilterMap = StdHashMap<TopicFilter, QoS>;
pub type TopicFilters = Vec<TopicFilter>;

#[derive(Debug, PartialEq, Clone)]
pub enum ConnectInfo {
    V3(Id, ConnectV3),
    V5(Id, Box<ConnectV5>),
}

impl ConnectInfo {

    #[inline]
    pub fn id(&self) -> &Id {
        match self {
            ConnectInfo::V3(id, _) => &id,
            ConnectInfo::V5(id, _) => &id,
        }
    }

    #[inline]
    pub fn to_json(&self) -> serde_json::Value {
        let json = match self {
            ConnectInfo::V3(id, conn_info) => {
                json!({
                    "node": id.node(),
                    "ipaddress": id.remote_addr,
                    "clientid": id.client_id,
                    "username": id.username,
                    "keepalive": conn_info.keep_alive,
                    "proto_ver": conn_info.protocol.level(),
                    "clean_session": conn_info.clean_session,
                    "last_will": self.last_will().map(|lw|lw.to_json())
                })
            }
            ConnectInfo::V5(id, conn_info) => {
                json!({
                    "node": id.node(),
                    "ipaddress": id.remote_addr,
                    "clientid": id.client_id,
                    "username": id.username,
                    "keepalive": conn_info.keep_alive,
                    "proto_ver": ntex_mqtt::types::MQTT_LEVEL_5,
                    "clean_start": conn_info.clean_start,
                    "last_will": self.last_will().map(|lw|lw.to_json()),

                    "session_expiry_interval_secs": conn_info.session_expiry_interval_secs,
                    "auth_method": conn_info.auth_method,
                    "auth_data": conn_info.auth_data,
                    "request_problem_info": conn_info.request_problem_info,
                    "request_response_info": conn_info.request_response_info,
                    "receive_max": conn_info.receive_max,
                    "topic_alias_max": conn_info.topic_alias_max,
                    "user_properties": conn_info.user_properties,
                    "max_packet_size": conn_info.max_packet_size,
                })
            }
        };
        json
    }

    #[inline]
    pub fn last_will(&self) -> Option<LastWill> {
        match self {
            ConnectInfo::V3(_, conn_info) => {
                conn_info.last_will.as_ref().map(|lw| LastWill::V3(lw))
            }
            ConnectInfo::V5(_, conn_info) => {
                conn_info.last_will.as_ref().map(|lw| LastWill::V5(lw))
            }
        }
    }

    #[inline]
    pub fn keep_alive(&self) -> u16 {
        match self {
            ConnectInfo::V3(_, conn_info) => conn_info.keep_alive,
            ConnectInfo::V5(_, conn_info) => conn_info.keep_alive,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PublishV3 {
    pub packet: v3::codec::Publish,
    pub topic: Topic,
    pub query: Option<ByteString>,
    pub create_time: TimestampMillis,
}

impl PublishV3 {

    #[inline]
    pub fn from(p: &v3::Publish) -> Result<PublishV3> {
        Ok(Self {
            packet: p.packet().clone(),
            topic: Topic::from_str(p.topic().get_ref())?,
            query: {
                let q = p.query();
                if q.is_empty() {
                    None
                } else {
                    Some(ByteString::from(q))
                }
            },
            create_time: chrono::Local::now().timestamp_millis(),
        })
    }

    #[inline]
    pub fn from_last_will(lw: &v3::codec::LastWill) -> Result<PublishV3> {
        let p = v3::codec::Publish {
            dup: false,
            retain: lw.retain,
            qos: lw.qos,
            topic: lw.topic.clone(),
            packet_id: None,
            payload: lw.message.clone(),
        };

        let (topic, query) = if let Some(pos) = lw.topic.find('?') {
            (
                //Topic::from_str(lw.topic.as_bytes().slice(0..pos))?,
                ByteString::try_from(lw.topic.as_bytes().slice(0..pos))?,
                Some(ByteString::try_from(
                    lw.topic.as_bytes().slice(pos + 1..lw.topic.len()),
                )?),
            )
        } else {
            (lw.topic.clone(), None)
        };

        Ok(Self {
            packet: p,
            topic: Topic::from_str(&topic)?,
            query,
            create_time: chrono::Local::now().timestamp_millis(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct PublishV5 {
    pub publish: v5::codec::Publish,
    pub topic: Topic,
    pub create_time: TimestampMillis,
}

impl PublishV5 {
    #[inline]
    pub fn from(publish: v5::codec::Publish) -> Result<PublishV5> {
        let topic = Topic::from_str(&publish.topic)?;
        Ok(Self {
            publish,
            topic,
            create_time: chrono::Local::now().timestamp_millis(),
        })
    }
}

pub trait QoSEx {
    fn value(&self) -> u8;
    fn less_value(&self, qos: QoS) -> QoS;
}

impl QoSEx for QoS {
    #[inline]
    fn value(&self) -> u8 {
        match self {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }
    }

    #[inline]
    fn less_value(&self, qos: QoS) -> QoS {
        if self.value() < qos.value() {
            *self
        } else {
            qos
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubscribeAclResult {
    V3(Vec<SubscribeReturnCodeV3>),
    V5(Vec<SubscribeAckReason>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PublishAclResult {
    Allow,
    Rejected(Disconnect),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AuthResult {
    BadUsernameOrPassword,
    NotAuthorized,
}

#[derive(Clone, Debug)]
pub enum Subscribe {
    V3(Vec<(TopicFilter, QoS)>),
    V5(SubscribeV5),
}

impl Subscribe {

    #[inline]
    pub fn adjust_topic_filters(&mut self, mut topic_filters: TopicFilters) -> Result<()> {
        if self.len() != topic_filters.len() {
            log::error!(
                "topic_filters quantity mismatch, {:?} <=> {:?}",
                self,
                topic_filters
            );
            return Err(MqttError::ServiceUnavailable);
        }

        match self {
            Subscribe::V3(subs) => {
                for (tf, _) in subs.iter_mut() {
                    *tf = topic_filters.remove(0);
                }
            }
            Subscribe::V5(subs) => {
                for (tf, _) in subs.topic_filters.iter_mut() {
                    *tf = ByteString::from(topic_filters.remove(0).to_string());
                }
            }
        }

        Ok(())
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Subscribe::V3(subs) => subs.len(),
            Subscribe::V5(subs) => subs.topic_filters.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool{
        self.len() == 0
    }

    #[inline]
    pub fn topic_filter(&self, idx: usize) -> Option<&TopicFilter> {
        match self {
            Subscribe::V3(subs) => subs.get(idx).map(|(tf, _)| tf),
            Subscribe::V5(_subs) => {
                log::warn!("[MQTT 5] Not implemented");
                None
            }
        }
    }

    #[inline]
    pub fn topic_filters(&self) -> Vec<(TopicFilter, QoS)> {
        match self {
            Subscribe::V3(subs) => subs
                .iter()
                .map(|(tf, qos)| (tf.clone(), *qos))
                .collect::<Vec<(TopicFilter, QoS)>>(),
            Subscribe::V5(subs) => {
                //@TODO ... TopicFilter
                subs.topic_filters
                    .iter()
                    .map(|(tf, opts)| (TopicFilter::from_str(tf).unwrap(), opts.qos))
                    .collect::<Vec<(TopicFilter, QoS)>>()
            }
        }
    }

    #[inline]
    pub fn remove(&mut self, topic_filter: &TopicFilter) {
        match self {
            Subscribe::V3(subs) => {
                *subs = subs
                    .drain(..)
                    .filter(|(tf, _)| tf != topic_filter)
                    .collect::<Vec<_>>();
            }
            Subscribe::V5(_subs) => {
                log::warn!("[MQTT 5] Not implemented");
            }
        }
    }

    #[inline]
    pub fn set_qos_if_less(&mut self, topic_filter: &TopicFilter, qos: QoS) {
        match self {
            Subscribe::V3(subs) => {
                for (tf, s_qos) in subs.iter_mut() {
                    if tf == topic_filter {
                        *s_qos = s_qos.less_value(qos);
                    }
                }
            }
            Subscribe::V5(_subs) => {
                log::warn!("[MQTT 5] Not implemented");
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SubscribeAck {
    V3(Vec<SubscribeReturnCodeV3>),
    V5(SubscribeAckV5),
}

impl SubscribeAck {

    #[inline]
    pub fn merge_from(&mut self, merged_ack: SubscribeAck) {
        match (self, merged_ack) {
            (SubscribeAck::V3(codes), SubscribeAck::V3(mut merged_codes)) => {
                if codes.len() != merged_codes.len() {
                    log::error!("SubscribeAck merge failed, SubscribeReturnCode inconsistent length  {:?} != {:?}", codes.len(), merged_codes.len());
                    return;
                }
                for (i, code) in codes.iter_mut().enumerate() {
                    *code = Self::v3_merge(code, merged_codes.remove(i));
                }
            }
            (SubscribeAck::V5(acks), SubscribeAck::V5(mut merged_acks)) => {
                if acks.status.len() != merged_acks.status.len() {
                    log::error!("SubscribeAck merge failed, SubscribeAckReason inconsistent length  {:?} != {:?}", acks.status.len(), merged_acks.status.len());
                    return;
                }
                //reason_string
                match (&mut acks.reason_string, merged_acks.reason_string) {
                    (Some(reason1), Some(reason2)) => {
                        acks.reason_string =
                            Some(ByteString::from(format!("{}, {}", reason1, reason2)))
                    }
                    (None, Some(reason)) => acks.reason_string = Some(reason),
                    (Some(_), None) => {}
                    (None, None) => {}
                };
                //properties
                for (prop_key, prop_val) in merged_acks.properties.drain(..) {
                    acks.properties.push((prop_key, prop_val))
                }
                //status
                for (i, status) in acks.status.iter_mut().enumerate() {
                    *status = Self::v5_merge(status, merged_acks.status.remove(i));
                }
            }
            _ => {
                log::error!("SubscribeAck merge failed, the type does not match");
                //unreachable!()
            }
        }
    }

    #[inline]
    fn v3_merge(
        code: &SubscribeReturnCodeV3,
        merged: SubscribeReturnCodeV3,
    ) -> SubscribeReturnCodeV3 {
        match (code, merged) {
            (_, SubscribeReturnCodeV3::Failure) => SubscribeReturnCodeV3::Failure,
            (SubscribeReturnCodeV3::Failure, _) => SubscribeReturnCodeV3::Failure,
            (SubscribeReturnCodeV3::Success(qos1), SubscribeReturnCodeV3::Success(qos2)) => {
                SubscribeReturnCodeV3::Success(qos1.less_value(qos2))
            }
        }
    }

    #[inline]
    fn v5_merge(status: &SubscribeAckReason, merged: SubscribeAckReason) -> SubscribeAckReason {
        log::warn!("[MQTT 5] Not implemented");

        if !matches!(
            status,
            SubscribeAckReason::GrantedQos0
                | SubscribeAckReason::GrantedQos1
                | SubscribeAckReason::GrantedQos2
        ) {
            return merged;
        }

        if !matches!(
            merged,
            SubscribeAckReason::GrantedQos0
                | SubscribeAckReason::GrantedQos1
                | SubscribeAckReason::GrantedQos2
        ) {
            return merged;
        }

        match (status, merged) {
            (_, SubscribeAckReason::GrantedQos0) => SubscribeAckReason::GrantedQos0,
            (&SubscribeAckReason::GrantedQos0, _) => SubscribeAckReason::GrantedQos0,
            (_, SubscribeAckReason::GrantedQos1) => SubscribeAckReason::GrantedQos1,
            (&SubscribeAckReason::GrantedQos1, _) => SubscribeAckReason::GrantedQos1,
            (&SubscribeAckReason::GrantedQos2, SubscribeAckReason::GrantedQos2) => {
                SubscribeAckReason::GrantedQos2
            }
            (_, _) => *status,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Subscribed {
    V3((Topic, QoS)),
    V5(SubscribedV5),
}

impl Subscribed {

    #[inline]
    pub fn topic_filter(&self) -> (TopicFilter, QoS) {
        match self {
            Subscribed::V3((t, qos)) => (t.clone(), *qos),
            Subscribed::V5(sub) => {
                //@TODO ... TopicFilter
                let (t, opts) = &sub.topic_filter;
                (TopicFilter::from_str(t).unwrap(), opts.qos)
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SubscribedV5 {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    /// Subscription Identifier
    pub id: Option<NonZeroU32>,
    pub user_properties: UserProperties,
    /// the list of Topic Filters and QoS to which the Client wants to subscribe.
    pub topic_filter: (ByteString, SubscriptionOptions),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConnectAckReason {
    V3(ConnectAckReasonV3),
    V5(ConnectAckReasonV5),
}

impl ConnectAckReason {

    #[inline]
    pub fn success(&self) -> bool {
        matches!(
            *self,
            ConnectAckReason::V3(ConnectAckReasonV3::ConnectionAccepted)
                | ConnectAckReason::V5(ConnectAckReasonV5::Success)
        )
    }

    #[inline]
    pub fn v3_error_ack<Io, St>(&self, handshake: v3::Handshake<Io>) -> HandshakeAckV3<Io, St> {
        match *self {
            ConnectAckReason::V3(ConnectAckReasonV3::UnacceptableProtocolVersion) => {
                handshake.service_unavailable()
            }
            ConnectAckReason::V3(ConnectAckReasonV3::IdentifierRejected) => {
                handshake.identifier_rejected()
            }
            ConnectAckReason::V3(ConnectAckReasonV3::ServiceUnavailable) => {
                handshake.service_unavailable()
            }
            ConnectAckReason::V3(ConnectAckReasonV3::BadUserNameOrPassword) => {
                handshake.bad_username_or_pwd()
            }
            ConnectAckReason::V3(ConnectAckReasonV3::NotAuthorized) => handshake.not_authorized(),
            ConnectAckReason::V3(ConnectAckReasonV3::Reserved) => handshake.service_unavailable(),
            _ => panic!("invalid value"),
        }
    }

    #[inline]
    pub fn reason(&self) -> &'static str {
        match *self {
            ConnectAckReason::V3(r) => r.reason(),
            ConnectAckReason::V5(r) => r.reason(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Unsubscribe {
    V3(Vec<TopicFilter>),
    V5(UnsubscribeV5),
}

impl Unsubscribe {

    #[inline]
    pub fn topic_filters(&self) -> Vec<TopicFilter> {
        match self {
            Unsubscribe::V3(unsubs) => unsubs.clone(),
            Unsubscribe::V5(unsubs) => {
                //@TODO ... TopicFilter
                unsubs
                    .topic_filters
                    .iter()
                    .map(|tf| TopicFilter::from_str(tf).unwrap())
                    .collect::<Vec<TopicFilter>>()
            }
        }
    }

    #[inline]
    pub fn adjust_topic_filters(&mut self, mut topic_filters: TopicFilters) -> Result<()> {
        if self.len() != topic_filters.len() {
            log::error!(
                "topic_filters quantity mismatch, {:?} <=> {:?}",
                self,
                topic_filters
            );
            return Err(MqttError::ServiceUnavailable);
        }

        match self {
            Unsubscribe::V3(unsubs) => {
                for tf in unsubs.iter_mut() {
                    *tf = topic_filters.remove(0);
                }
            }
            Unsubscribe::V5(unsubs) => {
                for tf in unsubs.topic_filters.iter_mut() {
                    *tf = ByteString::from(topic_filters.remove(0).to_string());
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Unsubscribe::V3(unsubs) => unsubs.len(),
            Unsubscribe::V5(unsubs) => unsubs.topic_filters.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone, Debug)]
pub enum UnsubscribeAck {
    V3,
    V5(UnsubscribeAckV5),
}

#[derive(Clone, Debug)]
pub enum Unsubscribed {
    V3(TopicFilter),
    V5(UnsubscribedV5),
}

impl Unsubscribed {
    #[inline]
    pub fn topic_filter(&self) -> TopicFilter {
        match self {
            Unsubscribed::V3(t) => t.clone(),
            Unsubscribed::V5(unsub) => {
                //@TODO ... TopicFilter
                TopicFilter::from_str(&unsub.topic_filter).unwrap()
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct UnsubscribedV5 {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    pub user_properties: UserProperties,
    /// the list of Topic Filters that the Client wishes to unsubscribe from.
    pub topic_filter: ByteString,
}

#[derive(Clone)]
pub enum LastWill<'a> {
    V3(&'a LastWillV3),
    V5(&'a LastWillV5),
}

impl<'a> fmt::Debug for LastWill<'a> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LastWill::V3(lw) => f
                .debug_struct("LastWill")
                .field("topic", &lw.topic)
                .field("retain", &lw.retain)
                .field("qos", &lw.qos.value())
                .field("message", &"<REDACTED>")
                .finish(),
            LastWill::V5(lw) => f
                .debug_struct("LastWill")
                .field("topic", &lw.topic)
                .field("retain", &lw.retain)
                .field("qos", &lw.qos.value())
                .field("message", &"<REDACTED>")
                .field("will_delay_interval_sec", &lw.will_delay_interval_sec)
                .field("correlation_data", &lw.correlation_data)
                .field("message_expiry_interval", &lw.message_expiry_interval)
                .field("content_type", &lw.content_type)
                .field("user_properties", &lw.user_properties)
                .field("is_utf8_payload", &lw.is_utf8_payload)
                .field("response_topic", &lw.response_topic)
                .finish(),
        }
    }
}

impl<'a> LastWill<'a> {

    #[inline]
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            LastWill::V3(lw) => {
                json!({
                    "qos": lw.qos.value(),
                    "retain": lw.retain,
                    "topic": lw.topic,
                    "message": base64::encode(lw.message.as_ref()),
                })
            }
            LastWill::V5(lw) => {
                json!({
                    "qos": lw.qos.value(),
                    "retain": lw.retain,
                    "topic": lw.topic,
                    "message": base64::encode(lw.message.as_ref()),

                    "will_delay_interval_sec": lw.will_delay_interval_sec,
                    "correlation_data": lw.correlation_data,
                    "message_expiry_interval": lw.message_expiry_interval,
                    "content_type": lw.content_type,
                    "user_properties": lw.user_properties,
                    "is_utf8_payload": lw.is_utf8_payload,
                    "response_topic": lw.response_topic,
                })
            }
        }
    }
}

impl<'a> Serialize for LastWill<'a> {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            LastWill::V3(lw) => {
                let mut s = serializer.serialize_struct("LastWill", 4)?;
                s.serialize_field("qos", &lw.qos.value())?;
                s.serialize_field("retain", &lw.retain)?;
                s.serialize_field("topic", &lw.topic)?;
                s.serialize_field("message", &lw.message)?;
                s.end()
            }
            LastWill::V5(lw) => {
                let mut s = serializer.serialize_struct("LastWill", 11)?;
                s.serialize_field("qos", &lw.qos.value())?;
                s.serialize_field("retain", &lw.retain)?;
                s.serialize_field("topic", &lw.topic)?;
                s.serialize_field("message", &lw.message)?;

                s.serialize_field("will_delay_interval_sec", &lw.will_delay_interval_sec)?;
                s.serialize_field("correlation_data", &lw.correlation_data)?;
                s.serialize_field("message_expiry_interval", &lw.message_expiry_interval)?;
                s.serialize_field("content_type", &lw.content_type)?;
                s.serialize_field("user_properties", &lw.user_properties)?;
                s.serialize_field("is_utf8_payload", &lw.is_utf8_payload)?;
                s.serialize_field("response_topic", &lw.response_topic)?;

                s.end()
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum Sink {
    V3(MqttSinkV3),
    V5(MqttSinkV5),
}

impl Sink {
    #[inline]
    pub(crate) fn close(&self) {
        match self {
            Sink::V3(s) => {
                s.close();
            }
            Sink::V5(s) => s.close(),
        }
    }

    #[inline]
    pub(crate) fn publish(&self, p: Publish) -> Result<()> {
        match self {
            Sink::V3(s) => {
                if let Publish::V3(p) = p {
                    s.send(v3::codec::Packet::Publish(p.packet))?;
                }
            }
            Sink::V5(_s) => {
                log::warn!("[MQTT 5] Not implemented");
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn send(&self, p: Packet) -> Result<()> {
        match self {
            Sink::V3(s) => {
                if let Packet::V3(p) = p {
                    s.send(p)?;
                }
            }
            Sink::V5(_s) => {
                if let Packet::V5(_p) = p {
                    //s.send(p)?;
                    log::warn!("[MQTT 5] Not implemented");
                }
            }
        }
        Ok(())
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Packet {
    V3(PacketV3),
    V5(PacketV5),
}

#[derive(Debug, Clone)]
pub enum Publish {
    V3(Box<PublishV3>),
    V5(Box<PublishV5>),
}

impl Publish {
    #[inline]
    pub fn payload(&self) -> &bytes::Bytes {
        match self {
            Publish::V3(p) => &p.packet.payload,
            Publish::V5(p) => &p.publish.payload,
        }
    }

    #[inline]
    pub fn retain(&self) -> bool {
        match self {
            Publish::V3(p) => p.packet.retain,
            Publish::V5(p) => p.publish.retain,
        }
    }

    #[inline]
    pub fn topic(&self) -> &Topic {
        match self {
            Publish::V3(p) => &p.topic,
            Publish::V5(p) => &p.topic,
        }
    }

    #[inline]
    pub fn dup(&self) -> bool {
        match self {
            Publish::V3(p) => p.packet.dup,
            Publish::V5(p) => p.publish.dup,
        }
    }

    #[inline]
    pub fn set_dup(&mut self, b: bool) {
        match self {
            Publish::V3(p) => p.packet.dup = b,
            Publish::V5(p) => p.publish.dup = b,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match self {
            Publish::V3(p) => p.packet.payload.is_empty(),
            Publish::V5(p) => p.publish.payload.is_empty(),
        }
    }

    #[inline]
    pub fn qos(&self) -> QoS {
        match self {
            Publish::V3(p) => p.packet.qos,
            Publish::V5(p) => p.publish.qos,
        }
    }

    #[inline]
    pub fn create_time(&self) -> TimestampMillis {
        match self {
            Publish::V3(p) => p.create_time,
            Publish::V5(p) => p.create_time,
        }
    }

    #[inline]
    pub fn packet_id(&self) -> Option<PacketId> {
        match self {
            Publish::V3(p) => p.packet.packet_id.map(|id| id.get()),
            Publish::V5(p) => p.publish.packet_id.map(|id| id.get()),
        }
    }

    #[inline]
    pub fn packet_id_is_none(&self) -> bool {
        match self {
            Publish::V3(p) => p.packet.packet_id.is_none(),
            Publish::V5(p) => p.publish.packet_id.is_none(),
        }
    }

    #[inline]
    pub fn set_packet_id(&mut self, packet_id: PacketId) {
        match self {
            Publish::V3(p) => p.packet.packet_id = NonZeroU16::new(packet_id),
            Publish::V5(p) => p.publish.packet_id = NonZeroU16::new(packet_id),
        }
    }
}

pub type From = Id;
pub type To = Id;

#[derive(Clone)]
pub struct Id(Arc<_Id>);

impl Id {

    #[inline]
    pub fn new(
        node_id: NodeId,
        local_addr: String,
        remote_addr: String,
        client_id: ClientId,
        username: Option<UserName>,
    ) -> Self {
        Self(Arc::new(_Id {
            id: ByteString::from(format!(
                "{}@{}/{}/{}",
                node_id, local_addr, remote_addr, client_id
            )),
            node_id,
            local_addr,
            remote_addr,
            client_id,
            username: username.unwrap_or_else(|| "undefined".into()),
        }))
    }

    #[inline]
    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "node": self.node(),
            "ipaddress": self.remote_addr,
            "clientid": self.client_id,
            "username": self.username,
        })
    }

    #[inline]
    pub fn from(client_id: ClientId) -> Self {
        Self::new(0, String::new(), String::new(), client_id, None)
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        &self.id
    }

    #[inline]
    pub fn node(&self) -> String {
        format!("{}/{}", self.node_id, self.local_addr)
    }
}

impl AsRef<str> for Id {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.id
    }
}

impl ToString for Id {
    #[inline]
    fn to_string(&self) -> String {
        self.id.to_string()
    }
}

impl std::fmt::Debug for Id {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl PartialEq<Id> for Id {
    #[inline]
    fn eq(&self, other: &Id) -> bool {
        self.id == other.id
    }
}
impl Eq for Id {}

impl std::hash::Hash for Id {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Deref for Id {
    type Target = _Id;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for Id {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        _Id::serialize(self.0.as_ref(), serializer)
    }
}

impl<'de> Deserialize<'de> for Id {
    #[inline]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Id(Arc::new(_Id::deserialize(deserializer)?)))
    }
}

#[derive(Default, Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub struct _Id {
    id: ByteString,
    pub node_id: NodeId,
    pub local_addr: String,
    pub remote_addr: String,
    pub client_id: ClientId,
    pub username: UserName,
}

#[derive(Debug, Clone)]
pub struct Retain {
    pub from: From,
    pub publish: Publish,
}

#[derive(Debug)]
pub enum Message {
    Forward(From, Publish),
    Kick(mpsc::UnboundedSender<()>, Id),
    Disconnect,
    Closed,
    Keepalive,
}
