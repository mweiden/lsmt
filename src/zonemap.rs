#[derive(Default)]
pub struct ZoneMap {
    pub min: Option<String>,
    pub max: Option<String>,
}

impl ZoneMap {
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

    pub fn contains(&self, key: &str) -> bool {
        match (self.min.as_deref(), self.max.as_deref()) {
            (Some(min), Some(max)) => key >= min && key <= max,
            _ => true,
        }
    }
}
