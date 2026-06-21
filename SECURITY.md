# Security Policy

Cowrie is a binary decoder and treats malformed inputs as security-relevant.

## Supported versions

Security fixes target the latest released minor line. Older unreleased tags or attic code are not supported.

## Reporting a vulnerability

Please report suspected vulnerabilities privately through GitHub Security Advisories for `Neumenon/cowrie`, or email the maintainer if advisories are unavailable. Include:

- affected language implementation(s),
- a minimal input or reproduction,
- observed behavior and expected rejection behavior,
- impact class: crash, memory exhaustion, data corruption, or parity drift.

Do not open a public issue for exploitable decoder crashes or resource-exhaustion bugs until a fix is available.

## Decoder hardening expectations

Every fix should add or update regression coverage for the relevant invariant: truncation, trailing data, depth/size limits, decompression bounds, unsafe integer conversion, or cross-language parity.
