# Error numbers

A user-visible error ends with "(error N)."; its server response carries `"number": N` and logs `error N Name` (no addresses, no secrets). Statuses and the user's own actions have no number. 1xx sign-in, 2xx review and checks, 3xx signing and confirmation, 4xx profile, 5xx page and network.

| N | Where | Meaning | User sentence |
|---|---|---|---|
| 101 | client auth.js | Wallet account lacks a public key, is not tz1/tz2/tz3 or not mainnet | Choose a Tezos mainnet tz1, tz2 or tz3 address (error 101). |
| 102 | server AuthError::Forbidden | Origin or CSRF check failed | Request rejected; reload the page (error 102). |
| 103 | server AuthError::InvalidSignature | Sign-in signature or public key did not verify | Sign-in failed; try again (error 103). |
| 104 | server AuthError::Expired | Challenge cookie missing, expired or used | Sign-in expired; try again (error 104). |
| 105 | server AuthError::RateLimited | Sign-in or profile-save limit, or write queue full | Too many attempts; wait a minute (error 105). |
| 106 | server AuthError::Unavailable | Sign-in state missing, client IP missing, render, cookie, entropy or save timeout | Sign-in is unavailable right now (error 106). |
| 107 | server AuthError::Configuration | Invalid app origin (startup) | Sign-in is unavailable right now (error 107). |
| 108 | client auth.js | At signing, wallet account is not the signed-in address on mainnet | Switch your wallet to the signed-in address (error 108). |
| 109 | server AuthError::Unauthenticated; client auth.js, market.js | No valid session | Sign in again (error 109). |
| 110 | client auth.js | Wallet error other than a cancel during sign-in | Sign-in failed in your wallet; try again (error 110). |
| 111 | server web/oauth.rs | Unknown OAuth client or return address | Unknown sign-in client or return address (error 111). |
| 201 | client market.js | Record ID outside the safe integer range | This record can't be handled here (error 201). |
| 202 | client market.js | Review fee breakdown or fee transfer did not validate | Something changed; review again (error 202). |
| 203 | client market.js | Checked operations do not match the reviewed fee transfer | Something changed; review again (error 203). |
| 221 | server FeeAuditUnavailable | Fee audit journal could not be opened | Accepting offers is unavailable right now (error 221). |
| 222 | server FeeUnconfigured | No platform fee address configured | Accepting offers is unavailable right now (error 222). |
| 223 | server FeeRecipient | Fee contract lacks a unit default entrypoint | Accepting offers is unavailable right now (error 223). |
| 224 | server RateLimited | Too many trading checks from the address or connection | Too many requests; wait a minute (error 224). |
| 225 | server SigningPending | A checked operation may still be signing | Your previous operation is still pending; wait N seconds (error 225). |
| 226 | server ReceiptCapacity | Receipt store full | Busy; try again in N seconds (error 226). |
| 227 | server Invalid | Malformed or unsupported trading request | Request failed (error 227). |
| 228 | server Unauthorized | Ticket or receipt belongs to another address or session | Sign in again (error 228). |
| 229 | server NotHeld | Address does not hold the token to accept with | Your address doesn't hold that NFT (error 229). |
| 230 | server Changed | Market record changed or inactive | This listing or offer changed; review it again (error 230). |
| 231 | server CodeChanged | Market contract code or configuration differs from the pinned review | Trading is unavailable right now (error 231). |
| 232 | server Unavailable | Chain data unavailable or stale, or trading off | Trading is unavailable right now; try again shortly (error 232). |
| 233 | server Busy | Another trading request of the address is active | Another request is still running; wait for it (error 233). |
| 234 | server Expired | Review ticket expired or used | The review expired; review again (error 234). |
| 235 | server Simulation | Simulation failed | This can't be done right now (error 235). |
| 236 | server TooLarge | Group or response exceeds a resource cap; bulk cancel splits on code `TooLarge` | Too many items; select fewer (error 236). |
| 237 | server Unrevealed | Address public key not revealed on chain | Your address must make one transaction before it can trade here (error 237). |
| 238 | client market.js | Session address differs from the profile at review | You signed in with another address; reload (error 238). |
| 239 | client market.js | Token ID field not a safe non-negative integer | Enter a valid token ID (error 239). |
| 301 | server Failed | Operation failed or dropped | The operation failed; nothing changed (error 301). |
| 302 | server Unverified | Operation does not match its review | The operation couldn't be confirmed; check it in your wallet (error 302). |
| 303 | client market.js | Receipt expired before the page could confirm | The operation couldn't be confirmed; check it in your wallet (error 303). |
| 304 | client market.js | Confirmation response neither confirmed nor pending | Request failed (error 304). |
| 305 | client market.js | Wallet returned no valid operation hash | Your wallet didn't return the operation; check your wallet before trying again (error 305). |
| 306 | client market.js | Bulk confirmation check failed after submission | Submitted; confirmation is delayed (error 306). |
| 307 | client market.js | Server no longer has the receipt (Expired/Unauthorized on confirm) | The operation couldn't be confirmed; check it in your wallet (error 307). |
| 308 | client auth.js | At signing, session address differs from the reviewed one | You signed in with another address; reload (error 308). |
| 309 | client auth.js | Wallet error other than a cancel during signing | Signing failed in your wallet (error 309). |
| 310 | server Pending | Awaiting canonical confirmation; a status, shown without number | Waiting for confirmation… |
| 320 | server FeeEvasion | Acceptance applied without the reviewed fee; log only, the page shows it as done | (none) |
| 401 | server web/auth.rs | Profile store unavailable or save failed | The profile wasn't saved; try again (error 401). |
| 402 | server web/auth.rs | Name registry check unavailable | The name couldn't be checked; try again later (error 402). |
| 403 | server web/auth.rs | Session ended before the save | Sign in again (error 403). |
| 404 | server web/auth.rs | Profile field rejected; the store names it | (store reason) (error 404). |
| 501 | client auth.js | Error response without a server sentence | Request failed; try again (error 501). |
| 502 | client auth.js | Success response without JSON | Request failed; try again (error 502). |
| 503 | client auth.js | Request timed out | Request timed out; try again (error 503). |
| 504 | client auth.js | Path outside the authenticated API | Request failed (error 504). |
| 505 | client auth.js | No response (network failure) | Request failed; try again (error 505). |
| 521 | client market.js | Next-page link leaves this page | Couldn't load the next page; reload (error 521). |
| 522 | client market.js | Next page failed, timed out or not OK | Couldn't load the next page; reload (error 522). |
| 523 | client market.js | Next page over 1 MiB | Couldn't load the next page; reload (error 523). |
| 524 | client market.js | Next page belongs to another signed-in address | You signed in with another address; reload (error 524). |
| 525 | server web/market.rs | Invalid market view, cursor or token filter | Invalid page (error 525). |
| 526 | server web/market.rs | Market page of another address | You signed in with another address (error 526). |
