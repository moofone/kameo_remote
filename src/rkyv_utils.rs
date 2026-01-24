use rkyv::{self, bytecheck::CheckBytes, Archive, Deserialize, Portable};

/// Safely access archived data from bytes without allocations.
pub fn access_archived<T>(bytes: &[u8]) -> Result<&T::Archived, rkyv::rancor::Error>
where
    T: Archive,
    for<'a> T::Archived: Portable
        + CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    rkyv::access::<T::Archived, rkyv::rancor::Error>(bytes)
}

/// Deserialize an archived value that has already been validated/accessed.
pub fn deserialize_archived<T>(archived: &T::Archived) -> Result<T, rkyv::rancor::Error>
where
    T: Archive,
    T::Archived:
        Deserialize<T, rkyv::rancor::Strategy<rkyv::de::pooling::Pool, rkyv::rancor::Error>>,
{
    let mut pool = rkyv::de::pooling::Pool::default();
    rkyv::api::deserialize_using(archived, &mut pool)
}

/// Deserialize bytes into an owned value by validating + accessing the archive.
///
/// # Deprecated
/// This allocates. Use `access_archived` or other zero-copy APIs instead.
#[cfg(any(test, feature = "allow-non-zero-copy"))]
#[deprecated(note = "Non-zero-copy; use access_archived/archived() zero-copy APIs instead.")]
pub fn deserialize_from_bytes<T>(bytes: &[u8]) -> Result<T, rkyv::rancor::Error>
where
    T: Archive,
    for<'a> T::Archived: Portable
        + CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + Deserialize<T, rkyv::rancor::Strategy<rkyv::de::pooling::Pool, rkyv::rancor::Error>>,
{
    let archived = access_archived::<T>(bytes)?;
    deserialize_archived::<T>(archived)
}
