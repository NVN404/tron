#!/usr/bin/env bash
set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}========================================================================${NC}"
echo -e "${CYAN}   MULTI-TENANT AGGREGATION LAYER: CYBERSECURITY HACKATHON LIVE DEMO   ${NC}"
echo -e "${CYAN}========================================================================${NC}\n"

echo -e "${BLUE}▶ PROOF 1: MULTI-TENANT ISOLATION & INC-02 ATTACK SUITE${NC}"
echo -e "Testing single-connection pool reuse, unauthenticated query leak, superuser gatekeeper..."
cargo test -p api --test security_attack_suite -- --test-threads=1
echo -e "${GREEN}✔ PROOF 1 PASSED: Zero cross-tenant data leakage under worst-case pool reuse.${NC}\n"

echo -e "${BLUE}▶ PROOF 2: MATHEMATICAL ACCURACY AGAINST DIRECT-SQL ORACLE${NC}"
echo -e "Comparing 10M record precomputed rollups against direct-SQL ground truth on source DB..."
cargo test -p worker --test backfill_tests
echo -e "${GREEN}✔ PROOF 2 PASSED: 100% exact numerical match across counts, sums, top-products, trends.${NC}\n"

echo -e "${BLUE}▶ PROOF 3: SELF-HEALING RECONCILIATION & DRIFT RECOVERY${NC}"
echo -e "Injecting artificial data corruption into analytics DB and triggering auto-heal..."
cargo test -p worker --test reconciliation_tests
echo -e "${GREEN}✔ PROOF 3 PASSED: Automatic drift detection, authoritative repair, and audit logging verified.${NC}\n"

echo -e "${BLUE}▶ PROOF 4: EMPIRICAL PERFORMANCE & FRESHNESS BENCHMARK${NC}"
echo -e "Executing empirical benchmarks on live 10,140,432 record dataset..."
cargo run -p bench

echo -e "${CYAN}========================================================================${NC}"
echo -e "${GREEN}   ALL LIVE DEMO PROOFS COMPLETED SUCCESSFULLY!                         ${NC}"
echo -e "${CYAN}========================================================================${NC}"

