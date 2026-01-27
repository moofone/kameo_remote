use rkyv::{self, bytecheck::CheckBytes, Archive, Deserialize, Portable};

// CRITICAL_PATH: archived access; zero-copy decoding depends on this path.
/// Safely access archived data from bytes (validated).
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

// CRITICAL_PATH: unchecked archived access (hot path).
/// Access archived data from bytes without validation.
///
/// # Safety
/// Callers must ensure the bytes are a valid archive for `T` and properly aligned.
pub fn access_archived_unchecked<T>(bytes: &[u8]) -> &T::Archived
where
    T: Archive,
    T::Archived: Portable,
{
    // SAFETY: Caller guarantees validity and alignment.
    unsafe { rkyv::access_unchecked::<T::Archived>(bytes) }
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
