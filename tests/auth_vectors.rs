use seeded_archive::auth::crypto::{packed_message, public_key_address, verify_signature};

#[test]
fn signatures_generated_by_independent_noble_curves_match_all_three_tezos_accounts() {
    // Noble curves / hashes and scure base generated these fixed vectors, independently
    // of the Rust signing and verification code. Synthetic seed: 32 bytes of 0x0b.
    let payload = "05010000004754657a6f73205369676e6564204d6573736167653a2068747470733a2f2f7365656465642e61727420323032362d31302d303520696e646570656e64656e742066697874757265";
    let vectors = [
        (
            "tz1SdoVSPFhnAGNMvqHn1MpzP7hkxEqDusPN",
            "edpkuRUPzyh4ZUnu9ggDyRQLdzMYpA3PRcABdSXhNGCMHQ7kkcrQW8",
            "edsigtsMevdLwLPXSeg2x3XiYTCbcD3HkoykQSsnpfdPVbFX6fnu9cWuEeVdnVewYxHFaVtgcUjZCHShfLstxRc7rPnnrb4S9ko",
        ),
        (
            "tz2Kbk1zvnuasH1JBvfr79zPeqSpesd5DPiL",
            "sppk7Zx9VgUD7NnNnFqKvbxQx729AZgDFZVaXUgr1f8cD6oDeQQdmdE",
            "spsig1AMWT1sXtaY3rDECrW2LcAkqvZKYEGnx79o6vB4M8bnXqy6iNL86QwzT9du4gnxm1Q6TxvwQwboMUarb593vUbNcTfjNNb",
        ),
        (
            "tz3bxaDU4gdXEMNhyfqMphLBnUNtA5gVfhCC",
            "p2pk66mGYZaFDKeftxaqhs4hGSrZncd763JZm2v8wzZxV3u6GDjf9Me",
            "p2sigd4xLAHxadxwxxViC67KRp2QXVxT4wngmUbJv9rn17ue7QK9PVQDV7DZz3gbKngRAPnt9DGpHXFVwNt5wqnLuBsMQQQ39z",
        ),
    ];
    for (address, key, signature) in vectors {
        assert_eq!(public_key_address(key).unwrap(), address);
        verify_signature(address, key, payload, signature).unwrap();
        assert!(verify_signature(address, key, &packed_message("changed"), signature).is_err());
    }
    assert_eq!(packed_message("€"), "050100000003e282ac");
}
