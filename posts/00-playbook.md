# OSS Launch Playbook: Methodology & Execution Guide

How we built Cowrie's launch materials, what each document does, the order to create them, and how to replicate this for any new repo.

---

## The Documents (in creation order)

### Phase 1: Understand What You Built

These are internal strategy docs. They never get posted anywhere. They inform everything else.

| # | Document | Purpose |
|---|----------|---------|
| 1 | `00-market-analysis.md` | What the code *actually* unlocks, market sizing, cost impact by customer size |
| 2 | `00-storage-cost-analysis.md` | Verified compression ratios from benchmarks, cloud pricing math, 7 cost scenarios |
| 3 | `00-target-company-profiles.md` | 20 specific companies profiled with tailored pitches, cost estimates, and POC scopes |
| 4 | `00-ideal-customer-profile.md` | General criteria for finding prospects: company traits, engineer persona, search queries, scoring rubric |

### Phase 2: Write the Launch Content

These get posted publicly. Each targets a different audience and platform.

| # | Document | Platform | Audience |
|---|----------|----------|----------|
| 5 | `01-blog-devto.md` | Dev.to / personal blog | Engineers evaluating the project technically |
| 6 | `02-show-hn.md` | Hacker News | Technical generalists, OSS enthusiasts |
| 7 | `03-reddit.md` | r/golang, r/rust, r/python | Language-specific communities |
| 8 | `04-github-release.md` | GitHub Releases | Users who find the repo directly |
| 9 | `05-twitter-thread.md` | Twitter/X | Broad reach, casual discovery |

---

## Phase 1 Methodology: Strategy Docs

### Doc 1: Market Analysis (`00-market-analysis.md`)

**What it is**: Deep analysis of what the code enables, who cares, and how big the opportunity is.

**How to create it for a new repo**:

1. **Read the actual code**, not just the README. List every capability the code provides -- not features, but *unlocks* (what was impossible/painful before that is now easy).
2. **For each unlock**, answer: Who has this pain today? What do they currently do instead? What does switching save them?
3. **Size the markets**. For each unlock, estimate:
   - Total addressable market (TAM) for the broader category
   - Serviceable addressable market (SAM) for the specific pain Cowrie addresses
   - Use public data: analyst reports, company filings, job postings as proxy for team size
4. **Calculate cost impact at 3 scales**: small team ($1-5M compute), medium ($5-50M), large ($50M+). Be specific about what changes at each scale.
5. **Be honest about what's incremental vs. novel**. Every project has both. Readers trust you more when you acknowledge the incremental parts.

**Inputs required**:
- Full codebase read (not just docs)
- Benchmark results from the test suite
- Competitor landscape (what exists today)

**Time**: ~4 hours of Claude work, mostly codebase analysis + web research for market data.

---

### Doc 2: Storage Cost Analysis (`00-storage-cost-analysis.md`)

**What it is**: Hard numbers on compression ratios from actual benchmarks, mapped to cloud pricing.

**How to create it for a new repo**:

1. **Run the benchmarks**. Every claim needs a source file and line number. If there are no benchmarks, write them first.
2. **Build a ratio table**: For each data pattern your tool handles, what's the before/after size? Document the test case, input size, output size, and ratio.
3. **Get current cloud pricing**: S3, transfer, compute, cache, streaming -- whatever services your users pay for. Pin the date.
4. **Build cost scenarios**: Pick 5-7 realistic use cases at different scales. For each:
   - Define assumptions (data volume, retention, replication factor)
   - Calculate before/after costs line by line
   - Show annual savings
5. **Include the multiplier effect**: $1 saved on serialization typically cascades to $8-21 total savings across storage, transfer, compute, and cache.

**Inputs required**:
- Benchmark test files from the codebase
- Cloud pricing pages (AWS, GCP, Azure)
- Representative data patterns from target users

**Time**: ~3 hours. The benchmark analysis is mechanical; the scenarios require judgment about realistic deployment patterns.

---

### Doc 3: Target Company Profiles (`00-target-company-profiles.md`)

**What it is**: 20 specific companies, each with: company card, current pain, your fit, cost estimate, tailored pitch, and POC scope.

**How to create it for a new repo**:

1. **Pick 4-5 verticals** where your tool applies. Don't force it -- if you only have 3 real verticals, use 3.
2. **Select 3-5 companies per vertical**. Mix of sizes: 1-2 large (validation), 2-3 mid-size (likely adopters), 1 small/startup (fast movers).
3. **For each company, research**:
   - Employee count, funding, revenue (public sources: Crunchbase, PitchBook, LinkedIn)
   - Tech stack (job postings, GitHub repos, eng blog posts)
   - Current serialization/storage/transport approach (GitHub issues, docs, blog posts)
   - Specific pain points with evidence (GitHub issue numbers, blog quotes)
4. **Map your features to their pain**. Rank which of your capabilities matter most *to them*, not to you.
5. **Calculate their specific savings** using ratios from Doc 2 and their estimated data volumes.
6. **Write the pitch pain-first**: "Your X does Y, which costs you Z. We fix that." Not "Our product does A, B, C."
7. **Design a POC**: 2-4 weeks, single team, one integration point, measurable success metric.

**Critical rules**:
- Every fact must be verifiable from public sources
- Pitches lead with pain, never features
- POCs must be technically feasible (not hand-wavy)
- Include one honest caveat per pitch
- Run 5 parallel research agents (one per vertical) to gather company data

**Inputs required**:
- Docs 1 and 2 completed
- Web research access for company data
- 5-10 hours for research + writing

**Time**: ~6-8 hours. The research is the bottleneck -- parallelize with subagents.

---

### Doc 4: Ideal Customer Profile (`00-ideal-customer-profile.md`)

**What it is**: General criteria for finding prospects -- abstracts the patterns from the 20 specific profiles into reusable search criteria.

**How to create it for a new repo**:

1. **Look across all 20 profiles** from Doc 3. What traits do the high-fit companies share? Extract the pattern.
2. **Define must-have traits** (3-4 things every good prospect has).
3. **Define strong signals** (6-8 observable behaviors that indicate fit).
4. **Define disqualifiers** (when to walk away).
5. **Profile the engineer** who adopts: title, what they're working on, where they hang out online.
6. **Write concrete search queries** for LinkedIn, GitHub, Google, HN -- copy-pasteable.
7. **Build a scoring rubric**: 5 dimensions, 0-3 each, with clear definitions for each score.
8. **Write opening conversations**: 3-5 archetypal conversations, each starting with a question about their pain.

**Inputs required**:
- Doc 3 completed (the patterns come from real profiles)
- Understanding of where your target engineers spend time online

**Time**: ~2 hours. This is synthesis, not research.

---

## Phase 2 Methodology: Launch Content

### Doc 5: Blog Post (`01-blog-devto.md`)

**What it is**: The definitive technical introduction. 1500-2500 words. This is what people link to.

**How to write it**:

1. **Open with the problem**, not your solution. 3 specific pain points the reader recognizes.
2. **Show code, not architecture diagrams**. Encode/decode in 3 languages, 10 lines each.
3. **Include benchmarks with methodology**. "We encoded 1,000 log events..." not "up to 95% smaller."
4. **Acknowledge alternatives honestly**. "If your schemas are stable and you have a platform team, Protobuf is great. Cowrie is for when..."
5. **End with a try-it snippet**. `go get` / `pip install` / `cargo add` + 5-line example.

**Tone**: Technical, specific, no hype. Write for the engineer who's skeptical but curious.

**Time**: ~2 hours.

---

### Doc 6: Show HN (`02-show-hn.md`)

**What it is**: Title + 200-word comment for Hacker News.

**How to write it**:

1. **Title**: "Show HN: [Name] -- [what it does in 10 words]". No marketing language.
2. **Comment**: What you built, why, one surprising benchmark, what's different from X, honest limitations.
3. **Prepare FAQ responses** for predictable questions: "Why not protobuf?", "Why not msgpack?", "How does this compare to X?"

**Rules**:
- No superlatives. "47% smaller" not "dramatically smaller."
- Anticipate "why not just use X" for every competitor.
- First comment should be the author explaining motivation.

**Time**: ~1 hour (the brevity is the hard part).

---

### Doc 7: Reddit Posts (`03-reddit.md`)

**What it is**: 3 separate posts tailored to language-specific subreddits.

**How to write it**:

1. **One post per subreddit**, each emphasizing that language's implementation.
2. **Lead with the language-specific detail**: Go gets `unsafe.Slice()` zero-copy, Rust gets `&[f32]` lifetime-safe views, Python gets `np.frombuffer()`.
3. **Mention dependencies and build impact**: Go devs care about dependency count, Rust devs care about compile time, Python devs care about pip install simplicity.
4. **Include a code snippet** that runs in that language alone -- don't require multi-language setup.

**Subreddit selection**: Pick 2-4 where your project is relevant. Don't spam irrelevant subs.

**Time**: ~1.5 hours.

---

### Doc 8: GitHub Release (`04-github-release.md`)

**What it is**: Structured release notes for the GitHub Releases page.

**How to write it**:

1. **Highlights section**: 3-5 bullet points, each one sentence.
2. **Feature table**: What each language supports, with checkmarks.
3. **Install commands**: One per language, copy-pasteable.
4. **Quick start code**: Smallest possible example that shows encode + decode.
5. **Benchmarks**: One comparison table (your format vs. JSON, ideally also vs. protobuf/msgpack).
6. **Links**: Docs, blog post, contributing guide.

**Time**: ~1 hour.

---

### Doc 9: Twitter Thread (`05-twitter-thread.md`)

**What it is**: 6-10 tweets that tell the story progressively.

**How to write it**:

1. **Tweet 1 (hook)**: What you shipped + repo link. No thread emoji, no "1/".
2. **Tweet 2-3 (problem)**: The pain, with a concrete example.
3. **Tweet 4-6 (solution)**: How it works, with a code screenshot or benchmark image.
4. **Tweet 7-8 (proof)**: Benchmark numbers or comparison table as an image.
5. **Tweet 9 (CTA)**: Link to blog post or repo.

**Rules**:
- Each tweet must stand alone (people see individual tweets in retweets).
- Numbers > adjectives. "47% smaller" not "much smaller."
- Suggest image/screenshot placements -- visual tweets get 2-3x engagement.

**Time**: ~45 minutes.

---

## Execution Order for a New Repo

```
Week 1: Foundation
  Day 1-2: Read the entire codebase. Run benchmarks. Understand what you built.
  Day 2-3: Write 00-market-analysis.md
  Day 3-4: Write 00-storage-cost-analysis.md

Week 2: Targeting
  Day 5-7: Write 00-target-company-profiles.md (parallelize research)
  Day 7:   Write 00-ideal-customer-profile.md (synthesize from profiles)

Week 3: Content
  Day 8:  Write 01-blog-devto.md (the cornerstone content)
  Day 9:  Write 02-show-hn.md + 03-reddit.md + 04-github-release.md
  Day 10: Write 05-twitter-thread.md

Launch Day:
  Morning:  Publish GitHub Release (04)
  Morning:  Publish blog post (01)
  Midday:   Post Show HN (02)
  Afternoon: Post Reddit (03)
  Evening:  Twitter thread (05)
```

---

## Claude Prompting Tips

### For Phase 1 docs (strategy)

- Always have Claude **read the actual codebase first** -- not just the README. The best insights come from understanding what the code does, not what the docs claim.
- Use **parallel subagents** for company research (one per vertical). Each agent searches web for company data independently.
- Have Claude **cite specific benchmark files and line numbers** for every compression ratio claim.
- Ask Claude to be **honest about limitations** -- "what's incremental vs. novel?" produces more trustworthy analysis.

### For Phase 2 docs (content)

- Write the **blog post first** -- it becomes the source material for all other content.
- For Show HN, ask Claude to **draft the top 5 skeptical comments** and prepare responses before posting.
- For Reddit, tell Claude the **specific subreddit norms** (r/golang hates self-promotion without substance, r/rust wants to see idiomatic code, r/python wants pip install simplicity).
- For Twitter, ask Claude to **suggest screenshot/image placements** -- text-only threads underperform.

### General

- Start every session with: "Read the codebase first, then we'll discuss strategy."
- Don't let Claude generate market numbers without web research -- insist on sourced data.
- For company profiles, always verify with: "Show me the public source for each fact."
- Keep strategy docs (`00-*`) separate from content docs (`01-05`) -- strategy is internal, content is public.
