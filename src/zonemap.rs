/// Zone map summarizing the minimum and maximum keys seen.
#[derive(Default)]
pub struct ZoneMap {
    /// Smallest key observed.
    pub min: Option<String>,
    /// Largest key observed.
    pub max: Option<String>,
}

impl ZoneMap {
    /// Update the zone map with a new `key`.
    pub fn update(&mut self, key: &str) {
        match self.min.as_deref() {
            Some(min) if key < min => self.min = Some(key.to_string()),
            None => self.min = Some(key.to_string()),
            _ => {}
        }
        match self.max.as_deref() {
            Some(max) if key > max => self.max = Some(key.to_string()),
            None => self.max = Some(key.to_string()),
            _ => {}
        }
    }

    /// Return `true` if `key` lies within the stored bounds. When bounds
    /// are missing the function defaults to `true` so as not to filter
    /// out potential matches.
    pub fn contains(&self, key: &str) -> bool {
        match (self.min.as_deref(), self.max.as_deref()) {
            (Some(min), Some(max)) => key >= min && key <= max,
            _ => true,
        }
    }

    /// Serialize the zone map to a byte vector separating the bounds
    /// with a newline. Missing bounds are encoded as empty strings.
    pub fn to_bytes(&self) -> Vec<u8> {
        let min = self.min.as_deref().unwrap_or("");
        let max = self.max.as_deref().unwrap_or("");
        [min.as_bytes(), &[b'\n'], max.as_bytes()].concat()
    }

    /// Reconstruct a zone map from the format produced by [`to_bytes`].
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut parts = data.splitn(2, |b| *b == b'\n');
        let min = parts.next().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(String::from_utf8_lossy(s).to_string())
            }
        });
        let max = parts.next().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(String::from_utf8_lossy(s).to_string())
            }
        });
        Self { min, max }
    }
}
