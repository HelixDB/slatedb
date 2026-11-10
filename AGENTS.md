# Writing instructions

Use the `simple-english` skill at `.agents/skills/simple-english/SKILL.md` for all English prose that you write.

This rule includes chat replies, documentation, new code comments, commit messages, and GitHub prose.
GitHub prose includes issue text, pull request titles and summaries, review comments, and discussion replies.
When you write a pull request description, follow `.github/pull_request_template.md`.

Read the complete skill file before you write or rewrite prose.
Apply its Plain mode unless the user requests STE, ASD-STE100, or compliance.
If the user requests Strict mode, read `.agents/skills/simple-english/references/strict-vocabulary.md` before you write.
If the user requests a text review, read `.agents/skills/simple-english/references/rule-catalog.md` before you report violations.
Follow the skill rules in every chat reply.
Do not edit an existing comment only to apply the skill.
Apply the skill to each new comment that you add.

# API design

Do not add `#[non_exhaustive]` to an enum only to avoid a future breaking change.
Prefer exhaustive enum matches, even when a new variant causes a breaking API change.

# Tests on Crucible

Run the tests on Crucible (https://crucibl.sh), not with a local `cargo nextest run`.
The `.mcp.json` file gives Claude Code the `crucible` MCP server.
Claude Code asks you one time to trust the server. Then sign in with `/mcp`, `crucible`, Authenticate.
For Codex, run `codex mcp add crucible --url https://mcp.crucibl.sh/mcp`.
If Crucible is not available, run the tests locally.
Crucible is not available when there are no `crucible` tools, sign-in fails, or `status` shows no ready base for `HelixDB/slatedb`.

1. Call `status` and take the `base.commit` of `HelixDB/slatedb`. Then run `git fetch origin <base.commit>`.
2. Call `create_diff_upload(repo: "HelixDB/slatedb", base: "<base.commit>")`. Then run the `command` that it returns, as given, from the repository root. The command uploads `git diff <base.commit>` and prints one line. Do not print the diff yourself.
3. Call `run_tests(repo: "HelixDB/slatedb", base: "<base.commit>", diff_upload: "<diff_upload>")`. To run one test, add `filters: ["<test name>"]`. An upload runs one time only, so do steps 2 and 3 again for each run.
4. If the answer is `{"status":"running","job_id":…}`, call `get_job(id: "<job_id>", wait_seconds: 45)` until the job ends.
5. If a test fails, read `failures`. Then call `get_job_logs(id: "<job_id>", grep: "FAIL|panicked|error[")`. Do not read the full log page by page.
6. Before you finish, run the tests one time without `filters`.

Clippy and `cargo fmt` stay local. Crucible runs only the tests.

If a result looks wrong, an error is unclear, something is slow or missing, or something works well, call `send_feedback`.
Give `kind`, `message`, and `job_id`. In `message`, write what you expected and what happened.
Do not include secrets or source code.
