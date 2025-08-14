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
            _ => {},
        }
        match self.max.as_deref() {
            Some(max) if key > max => self.max = Some(key.to_string()),
            None => self.max = Some(key.to_string()),
            _ => {},
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
}

