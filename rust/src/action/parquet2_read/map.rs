use std::collections::HashMap;

use parquet2::metadata::ColumnDescriptor;
use parquet2::page::DataPage;
use parquet2::schema::types::GroupConvertedType;
use parquet2::schema::types::ParquetType;

use super::{ActionVariant, ParseError};
use crate::action::Action;

#[inline]
pub fn for_each_map_field_value<ActType, SetFn>(
    _actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    _set_fn: SetFn,
) -> Result<(), ParseError>
where
    ActType: ActionVariant,
    SetFn: Fn((&mut ActType, HashMap<String, Option<String>>)),
{
    let _fields = if let ParquetType::GroupType {
        converted_type,
        fields,
        ..
    } = descriptor.type_()
    {
        match converted_type {
            Some(GroupConvertedType::Map) | Some(GroupConvertedType::MapKeyValue) => Ok(fields),
            _ => Err(ParseError::InvalidAction(format!(
                "expect Map converted type, got: {:?}",
                converted_type,
            ))),
        }
    } else {
        Err(ParseError::InvalidAction(format!(
            "expect Map group type, got: {:?}",
            descriptor.type_(),
        )))
    }?;

    dbg!(descriptor);
    dbg!(page);
    panic!("nooo");
    // Ok(())
}
