use crate::error::{FlapjackError, Result};
use dashmap::DashMap;
use instant_acme::{
    Account, AuthorizationStatus, ChallengeType, Identifier, LetsEncrypt, NewAccount, NewOrder,
    Order, RetryPolicy,
};
use std::net::IpAddr;
use std::sync::Arc;

/// ACME client for handling Let's Encrypt certificate operations
pub struct AcmeClient {
    /// ACME account (persisted)
    account: Arc<Account>,
    /// Challenge responses (token -> key_authorization)
    /// Stored in-memory during http-01 validation
    challenges: Arc<DashMap<String, String>>,
}

impl AcmeClient {
    /// Create a new ACME client or load existing account
    pub async fn new(email: &str, acme_directory: &str) -> Result<Self> {
        tracing::info!("[SSL] Initializing ACME client for {}", email);

        let directory_url = if acme_directory.contains("staging") {
            LetsEncrypt::Staging.url().to_owned()
        } else {
            LetsEncrypt::Production.url().to_owned()
        };
        tracing::info!("[SSL] Using ACME directory {}", directory_url);

        // Create a new account
        let (account, _credentials) = Account::builder()?
            .create(
                &NewAccount {
                    contact: &[&format!("mailto:{}", email)],
                    terms_of_service_agreed: true,
                    only_return_existing: false,
                },
                directory_url.clone(),
                None,
            )
            .await
            .map_err(|e| FlapjackError::Acme(format!("Failed to create ACME account: {}", e)))?;

        tracing::info!("[SSL] ACME account created successfully");

        Ok(Self {
            account: Arc::new(account),
            challenges: Arc::new(DashMap::new()),
        })
    }

    /// Request a new certificate for the given IP address
    /// Returns (certificate_pem, private_key_pem)
    pub async fn request_certificate(&self, ip: &str) -> Result<(String, String)> {
        tracing::info!("[SSL] Requesting certificate for IP: {}", ip);

        let ip_addr = Self::parse_requested_ip(ip)?;
        let mut order = self.create_order(ip_addr).await?;
        let order_tokens = self.prepare_http01_challenges(&mut order).await?;
        self.wait_for_order_ready(&mut order, &order_tokens).await?;
        self.finalize_order(&mut order).await
    }

    /// Get the challenge response for a given token (used by HTTP handler)
    pub fn get_challenge_response(&self, token: &str) -> Option<String> {
        self.challenges.get(token).map(|v| v.clone())
    }

    fn parse_requested_ip(ip: &str) -> Result<IpAddr> {
        ip.parse()
            .map_err(|e| FlapjackError::Acme(format!("Invalid IP address: {}", e)))
    }

    async fn create_order(&self, ip_addr: IpAddr) -> Result<Order> {
        let identifier = Identifier::Ip(ip_addr);
        let order = self
            .account
            .new_order(&NewOrder::new(&[identifier]))
            .await
            .map_err(|e| FlapjackError::Acme(format!("Failed to create ACME order: {}", e)))?;

        tracing::info!("[SSL] ACME order created");
        Ok(order)
    }

    async fn prepare_http01_challenges(&self, order: &mut Order) -> Result<Vec<String>> {
        let mut order_tokens = Vec::new();
        let mut authorizations = order.authorizations();

        while let Some(authz_result) = authorizations.next().await {
            let mut authz = authz_result
                .map_err(|e| FlapjackError::Acme(format!("Failed to get authorization: {}", e)))?;

            if matches!(authz.status, AuthorizationStatus::Valid) {
                continue;
            }

            let mut challenge = authz
                .challenge(ChallengeType::Http01)
                .ok_or_else(|| FlapjackError::Acme("No http-01 challenge found".to_string()))?;

            let token = challenge.token.clone();
            let key_authorization = challenge.key_authorization().as_str().to_string();
            self.challenges.insert(token.clone(), key_authorization);
            order_tokens.push(token.clone());

            tracing::info!("[SSL] Stored http-01 challenge token: {}", token);

            challenge.set_ready().await.map_err(|e| {
                FlapjackError::Acme(format!("Failed to set challenge ready: {}", e))
            })?;

            tracing::info!("[SSL] Challenge marked as ready, waiting for validation...");
        }

        Ok(order_tokens)
    }

    async fn wait_for_order_ready(&self, order: &mut Order, order_tokens: &[String]) -> Result<()> {
        tracing::info!("[SSL] Polling for order ready status...");
        let poll_result = order.poll_ready(&RetryPolicy::default()).await;

        Self::cleanup_challenge_tokens(&self.challenges, order_tokens);
        tracing::debug!("[SSL] Cleaned up {} challenge tokens", order_tokens.len());

        poll_result
            .map(|_| ())
            .map_err(|e| FlapjackError::Acme(format!("Failed to poll order ready: {}", e)))
    }

    async fn finalize_order(&self, order: &mut Order) -> Result<(String, String)> {
        tracing::info!("[SSL] Finalizing order...");
        let private_key_pem = order
            .finalize()
            .await
            .map_err(|e| FlapjackError::Acme(format!("Failed to finalize order: {}", e)))?;

        tracing::info!("[SSL] Polling for certificate...");
        let cert_chain_pem = order
            .poll_certificate(&RetryPolicy::default())
            .await
            .map_err(|e| FlapjackError::Acme(format!("Failed to poll certificate: {}", e)))?;

        tracing::info!("[SSL] Certificate issued successfully");
        Ok((cert_chain_pem, private_key_pem))
    }

    fn cleanup_challenge_tokens(challenges: &DashMap<String, String>, order_tokens: &[String]) {
        for token in order_tokens {
            challenges.remove(token);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_requested_ip_accepts_ip_addresses() {
        let ip = AcmeClient::parse_requested_ip("127.0.0.1").unwrap();
        assert_eq!(ip, IpAddr::from([127, 0, 0, 1]));
    }

    #[test]
    fn parse_requested_ip_rejects_invalid_addresses() {
        let error = AcmeClient::parse_requested_ip("not-an-ip").unwrap_err();
        assert!(matches!(error, FlapjackError::Acme(_)));
    }

    #[test]
    fn cleanup_challenge_tokens_removes_only_order_tokens() {
        let challenges = DashMap::new();
        challenges.insert("keep".to_string(), "value-1".to_string());
        challenges.insert("drop-1".to_string(), "value-2".to_string());
        challenges.insert("drop-2".to_string(), "value-3".to_string());

        AcmeClient::cleanup_challenge_tokens(
            &challenges,
            &["drop-1".to_string(), "drop-2".to_string()],
        );

        assert!(challenges.contains_key("keep"));
        assert!(!challenges.contains_key("drop-1"));
        assert!(!challenges.contains_key("drop-2"));
    }
}
