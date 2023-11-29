//! `SeaORM` Entity. Generated by sea-orm-codegen 0.12.4

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "write_set_changes_module")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub transaction_version: String,
    pub transaction_block_height: i64,
    pub hash: String,
    pub write_set_change_type: String,
    pub address: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub index: i64,
    pub is_deleted: bool,
    #[sea_orm(column_type = "Binary(BlobSize::Blob(None))", nullable)]
    pub bytecode: Option<Vec<u8>>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub friends: Option<Json>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub exposed_functions: Option<Json>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub structs: Option<Json>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
