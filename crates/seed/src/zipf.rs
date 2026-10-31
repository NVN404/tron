use rand::Rng;

/// Precomputed cumulative discrete distribution for Zipfian tenant sampling.
#[derive(Debug, Clone)]
pub struct ZipfSampler {
    cumulative_weights: Vec<f64>,
}

impl ZipfSampler {
    pub fn new(n: usize, s: f64) -> Self {
        let mut weights = Vec::with_capacity(n);
        let mut sum = 0.0;
        for i in 1..=n {
            let w = 1.0 / (i as f64).powf(s);
            sum += w;
            weights.push(sum);
        }
        // Normalize cumulative weights to 1.0
        let cumulative_weights = weights.into_iter().map(|w| w / sum).collect();
        Self { cumulative_weights }
    }

    /// Samples a rank in 0..n based on the precomputed cumulative weights.
    pub fn sample<R: Rng>(&self, rng: &mut R) -> usize {
        let p: f64 = rng.gen();
        match self.cumulative_weights.binary_search_by(|w| w.partial_cmp(&p).unwrap()) {
            Ok(idx) => idx,
            Err(idx) => idx.min(self.cumulative_weights.len() - 1),
        }
    }
}
