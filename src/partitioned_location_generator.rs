use iceberg::{
    Error, ErrorKind, spec::TableMetadata,
    writer::file_writer::location_generator::LocationGenerator,
};

const WRITE_DATA_LOCATION: &str = "write.data.path";
const WRITE_FOLDER_STORAGE_LOCATION: &str = "write.folder-storage.path";
const DEFAULT_DATA_DIR: &str = "/data";

#[derive(Clone, Debug)]
pub struct PartitionedLocationGenerator {
    dir_path: String,
    partitions: Vec<(String, String)>,
}

impl PartitionedLocationGenerator {
    pub fn new(
        table_metadata: TableMetadata,
        partition_spec_id: i32,
        part_values: Vec<String>,
    ) -> anyhow::Result<Self> {
        let part_spec = table_metadata
            .partition_spec_by_id(partition_spec_id)
            .ok_or_else(|| anyhow::anyhow!("Partition spec {} not found", partition_spec_id))?;

        let part_fields = part_spec.fields();

        if part_fields.len() != part_values.len() {
            anyhow::bail!(
                "Partition spec {} mismatch: {} fields vs {} values",
                partition_spec_id,
                part_fields.len(),
                part_values.len()
            );
        }

        let mut partitions = vec![];

        for (i, part_field) in part_fields.into_iter().enumerate() {
            partitions.push((part_field.name.clone(), part_values.get(i).unwrap().clone()));
        }

        let table_location = table_metadata.location();
        let rel_dir_path = {
            let prop = table_metadata.properties();
            let data_location = prop
                .get(WRITE_DATA_LOCATION)
                .or(prop.get(WRITE_FOLDER_STORAGE_LOCATION));
            if let Some(data_location) = data_location {
                data_location.strip_prefix(table_location).ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "data location {} is not a subpath of table location {}",
                            data_location, table_location
                        ),
                    )
                })?
            } else {
                DEFAULT_DATA_DIR
            }
        };

        Ok(Self {
            dir_path: format!("{}{}", table_location, rel_dir_path),
            partitions,
        })
    }
}

impl LocationGenerator for PartitionedLocationGenerator {
    fn generate_location(&self, file_name: &str) -> String {
        let part_dir = self
            .partitions
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("/");

        format!("{}/{}/{}", self.dir_path, part_dir, file_name)
    }
}
