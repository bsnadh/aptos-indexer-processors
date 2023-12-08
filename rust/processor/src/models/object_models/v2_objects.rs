// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    models::{
        default_models::move_resources::MoveResource,
        fungible_asset_models::v2_fungible_asset_utils::{
            FungibleAssetMetadata, FungibleAssetStore, FungibleAssetSupply,
        },
        token_models::collection_datas::{QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
        token_v2_models::v2_token_utils::{
            AptosCollection, FixedSupply, PropertyMapModel, TokenV2, TransferEvent,
            UnlimitedSupply, V2TokenResource,
        },
    },
    schema::{current_objects, objects},
    utils::{
        database::PgPoolConnection,
        util::{deserialize_from_string, standardize_address},
    },
};
use aptos_protos::transaction::v1::{DeleteResource, WriteResource};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::warn;

// PK of current_objects, i.e. object_address
pub type CurrentObjectPK = String;

/// Tracks all object related metadata in a hashmap for quick access (keyed on address of the object)
pub type ObjectAggregatedDataMapping = HashMap<CurrentObjectPK, ObjectAggregatedData>;

/// Index of the event so that we can write its inverse to the db as primary key (to avoid collisiona)
pub type EventIndex = i64;

/// This contains metadata for the object
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ObjectAggregatedData {
    pub object: ObjectWithMetadata,
    pub transfer_event: Option<(EventIndex, TransferEvent)>,
    // Fungible asset structs
    pub fungible_asset_metadata: Option<FungibleAssetMetadata>,
    pub fungible_asset_supply: Option<FungibleAssetSupply>,
    pub fungible_asset_store: Option<FungibleAssetStore>,
    // Token v2 structs
    pub aptos_collection: Option<AptosCollection>,
    pub fixed_supply: Option<FixedSupply>,
    pub property_map: Option<PropertyMapModel>,
    pub token: Option<TokenV2>,
    pub unlimited_supply: Option<UnlimitedSupply>,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = objects)]
pub struct Object {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub guid_creation_num: BigDecimal,
    pub allow_ungated_transfer: bool,
    pub is_token: bool,
    pub is_fungible_asset: bool,
    pub is_deleted: bool,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(object_address))]
#[diesel(table_name = current_objects)]
pub struct CurrentObject {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: BigDecimal,
    pub last_transaction_version: i64,
    pub is_token: bool,
    pub is_fungible_asset: bool,
    pub is_deleted: bool,
}

#[derive(Debug, Deserialize, Identifiable, Queryable, Serialize)]
#[diesel(primary_key(object_address))]
#[diesel(table_name = current_objects)]
pub struct CurrentObjectQuery {
    pub object_address: String,
    pub owner_address: String,
    pub state_key_hash: String,
    pub allow_ungated_transfer: bool,
    pub last_guid_creation_num: BigDecimal,
    pub last_transaction_version: i64,
    pub is_deleted: bool,
    pub inserted_at: chrono::NaiveDateTime,
    pub is_token: bool,
    pub is_fungible_asset: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ObjectCore {
    pub allow_ungated_transfer: bool,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub guid_creation_num: BigDecimal,
    owner: String,
}

impl ObjectCore {
    pub fn get_owner_address(&self) -> String {
        standardize_address(&self.owner)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ObjectWithMetadata {
    pub object_core: ObjectCore,
    state_key_hash: String,
}

impl ObjectWithMetadata {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
    ) -> anyhow::Result<Option<Self>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !V2TokenResource::is_resource_supported(type_str.as_str()) {
            return Ok(None);
        }
        if let V2TokenResource::ObjectCore(inner) = V2TokenResource::from_resource(
            &type_str,
            &serde_json::from_str(write_resource.data.as_str()).unwrap(),
            txn_version,
        )? {
            Ok(Some(Self {
                object_core: inner,
                state_key_hash: standardize_address(
                    hex::encode(write_resource.state_key_hash.as_slice()).as_str(),
                ),
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_state_key_hash(&self) -> String {
        standardize_address(&self.state_key_hash)
    }
}

impl Object {
    pub fn from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        object_metadata_mapping: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<(Self, CurrentObject)>> {
        let address = standardize_address(&write_resource.address.to_string());
        if let Some(object_aggregated_metadata) = object_metadata_mapping.get(&address) {
            // do something
            let object_with_metadata = object_aggregated_metadata.object.clone();
            let object_core = object_with_metadata.object_core;
            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    object_address: address.clone(),
                    owner_address: object_core.get_owner_address(),
                    state_key_hash: object_with_metadata.state_key_hash.clone(),
                    guid_creation_num: object_core.guid_creation_num.clone(),
                    allow_ungated_transfer: object_core.allow_ungated_transfer,
                    is_token: object_aggregated_metadata.token.is_some(),
                    is_fungible_asset: object_aggregated_metadata.fungible_asset_store.is_some(),
                    is_deleted: false,
                },
                CurrentObject {
                    object_address: address,
                    owner_address: object_core.get_owner_address(),
                    state_key_hash: object_with_metadata.state_key_hash,
                    allow_ungated_transfer: object_core.allow_ungated_transfer,
                    last_guid_creation_num: object_core.guid_creation_num.clone(),
                    last_transaction_version: txn_version,
                    is_token: object_aggregated_metadata.token.is_some(),
                    is_fungible_asset: object_aggregated_metadata.fungible_asset_store.is_some(),
                    is_deleted: false,
                },
            )))
        } else {
            Ok(None)
        }
    }

    /// This handles the case where the entire object is deleted
    /// TODO: We need to detect if an object is only partially deleted
    /// using KV store
    pub async fn from_delete_resource(
        delete_resource: &DeleteResource,
        txn_version: i64,
        write_set_change_index: i64,
        object_mapping: &HashMap<CurrentObjectPK, CurrentObject>,
        conn: &mut PgPoolConnection<'_>,
    ) -> anyhow::Result<Option<(Self, CurrentObject)>> {
        if delete_resource.type_str == "0x1::object::ObjectGroup" {
            let resource = MoveResource::from_delete_resource(
                delete_resource,
                0, // Placeholder, this isn't used anyway
                txn_version,
                0, // Placeholder, this isn't used anyway
            );
            let previous_object = if let Some(object) = object_mapping.get(&resource.address) {
                object.clone()
            } else {
                Self::get_object(conn, &resource.address, txn_version).await
            };
            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    object_address: resource.address.clone(),
                    owner_address: previous_object.owner_address.clone(),
                    state_key_hash: resource.state_key_hash.clone(),
                    guid_creation_num: previous_object.last_guid_creation_num.clone(),
                    allow_ungated_transfer: previous_object.allow_ungated_transfer,
                    is_token: previous_object.is_token,
                    is_fungible_asset: previous_object.is_fungible_asset,
                    is_deleted: true,
                },
                CurrentObject {
                    object_address: resource.address,
                    owner_address: previous_object.owner_address.clone(),
                    state_key_hash: resource.state_key_hash,
                    last_guid_creation_num: previous_object.last_guid_creation_num.clone(),
                    allow_ungated_transfer: previous_object.allow_ungated_transfer,
                    last_transaction_version: txn_version,
                    is_token: previous_object.is_token,
                    is_fungible_asset: previous_object.is_fungible_asset,
                    is_deleted: true,
                },
            )))
        } else {
            Ok(None)
        }
    }

    /// This is actually not great because object owner can change. The best we can do now though.
    /// This will loop forever until we get the object from the db
    pub async fn get_object(
        conn: &mut PgPoolConnection<'_>,
        object_address: &str,
        transaction_version: i64,
    ) -> CurrentObject {
        let retries = 0;
        while retries < QUERY_RETRIES {
            match CurrentObjectQuery::get_by_address(object_address, conn).await {
                Ok(res) => {
                    return CurrentObject {
                        object_address: res.object_address,
                        owner_address: res.owner_address,
                        state_key_hash: res.state_key_hash,
                        allow_ungated_transfer: res.allow_ungated_transfer,
                        last_guid_creation_num: res.last_guid_creation_num,
                        last_transaction_version: res.last_transaction_version,
                        is_token: res.is_token,
                        is_fungible_asset: res.is_fungible_asset,
                        is_deleted: res.is_deleted,
                    };
                },
                Err(e) => {
                    std::thread::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS));
                    warn!(
                        transaction_version,
                        error = ?e,
                        "Failed to get object from current_objects table for object_address: {}, retrying in {} ms. ",
                        object_address,
                        QUERY_RETRY_DELAY_MS,
                    );
                },
            }
        }
        panic!("Failed to get object from current_objects table for object_address: {}. You should probably backfill db.", object_address);
    }
}

impl CurrentObjectQuery {
    /// TODO: Change this to a KV store
    pub async fn get_by_address(
        object_address: &str,
        conn: &mut PgPoolConnection<'_>,
    ) -> diesel::QueryResult<Self> {
        current_objects::table
            .filter(current_objects::object_address.eq(object_address))
            .first::<Self>(conn)
            .await
    }
}
