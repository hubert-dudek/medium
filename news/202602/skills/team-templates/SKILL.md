---
name: team-templates
description: Generate standardized team templates (project kickoff, data contract, incident postmortem/RCA, weekly status update, Unity Catalog access request). Use when someone asks for a template, PRD/1-pager, design doc, postmortem, weekly update, data contract, or access request.
license: Proprietary. For internal team use.
compatibility: Designed for Databricks Assistant agent mode in Databricks workspaces.
metadata:
  author: your-team
  version: "1.0"
---

# Team templates library

Use this skill to generate consistent, copy/paste-ready templates for common data/analytics team workflows.

## Template catalog

Pick the best match based on the user request:

- **Project kickoff / 1‑pager** → `assets/kickoff.md`
- **Weekly status update** (Slack + longer-form) → `assets/weekly-status-update.md`
- **Incident postmortem / RCA** (Sev/Outage) → `assets/incident-postmortem.md`
- **Data contract** (dataset/table contract in YAML) → `assets/data-contract.yml`
- **Unity Catalog access request** (permissions + optional GRANT SQL) → `assets/unity-catalog-access-request.md`

## How to use this skill

### 1) Identify the template type
If the user explicitly names one (kickoff, postmortem, weekly update, data contract, access request), use it.

If not explicit, infer from keywords:
- *“RCA”, “postmortem”, “outage”, “incident”, “Sev”* → incident postmortem
- *“1‑pager”, “kickoff”, “proposal”, “PRD”, “new project”* → kickoff doc
- *“data contract”, “schema contract”, “dataset contract”* → data contract YAML
- *“grant”, “permission”, “Unity Catalog”, “access to schema/table”* → UC access request
- *“weekly update”, “status”, “what did we do this week”* → weekly status update

### 2) Ask for only missing inputs (max 3 questions)
If required details are missing, ask up to **three** targeted questions. Prefer defaults over interrogations.

Common inputs:
- Names: project/dataset/table, team, owner
- Dates: start date, incident date/time range
- Audience: internal team vs stakeholders vs exec summary
- Access scope: catalog.schema.table, permission level (SELECT/MODIFY/OWN), duration

If the user provided none, assume:
- Audience: “internal + stakeholders”
- Timezone: user local (unknown) → label as “local time”
- Dates: today’s date is fine **only if user asked “today”**; otherwise leave placeholders.

### 3) Generate a filled template (deliverable-first)
Output the chosen template in **Markdown** (or YAML for the data contract), with placeholders replaced when you have information.

Rules:
- Keep headings intact so the doc is scannable.
- Keep checklists as `- [ ]` so teams can track work.
- Use neutral language. Avoid blaming individuals.

### 4) Add a short “Next steps” section
After the template, add 3–8 concrete next steps tailored to the template type.

## Examples

### Example A — kickoff
**User:** “Create a kickoff doc for a new customer churn dashboard in Databricks.”

**Assistant output:** A filled version of `assets/kickoff.md` including goals, stakeholders, datasets, milestones, and a definition of done.

### Example B — postmortem
**User:** “We had a 2‑hour outage because the DLT pipeline failed. Draft the postmortem.”

**Assistant output:** A filled version of `assets/incident-postmortem.md` with impact, timeline, root cause hypotheses, and action items.

## Edge cases

- **User wants Confluence/Google Doc formatting:** Output Markdown, but keep headings and checklists; it will paste cleanly.
- **User wants multiple templates:** Output one at a time; ask which is highest priority.
- **User provides sensitive data:** Keep it high-level; replace secrets with `[REDACTED]`.
- **Unknown incident details:** Keep placeholders and add a section “Information needed” listing what’s missing.
