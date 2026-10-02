# ai_decide — short-video screenplay

**Length:** 39 seconds. **Primary format:** 1080 × 1920, with a square LinkedIn adaptation. **Language:** English. **Source:** `src/01_ai_decide.py` and `resources/job.yml`.

## Visual style

Match `v6.png`: white canvas, very large pure-red (`#FF0000`) captions across the top, and thin red rectangular outlines around the exact code or result being discussed. Use an unfilled 3–4 px outline; an occasional red arrow can connect a question to its answer. Keep captions to two short lines, around 90–110 px on the portrait canvas. Reserve the top quarter for captions and keep the bottom clear of Shorts controls.

Crop the notebook to the active cell or table. Use a brief zoom in to direct attention, then zoom out before changing context. Never fit the entire notebook into one portrait frame. Keep code readable; show one question definition at a time. For a square version, reframe the same captures with a shallower title band and retain readable code sizes.

## Shot list

` / ` marks a caption line break. All values shown in result grids must come from a real run of this demo.

| Time | Exact large red caption | Capture and action | Zoom / red highlight |
| --- | --- | --- | --- |
| 00:00–00:03 | `ai_decide / IN SQL` | Open on the populated result table: `ticket_id`, `team`, `escalation_probability`, `urgency_score`. Lead with the useful output. | Start already close enough to read. Outline one actual ticket result. If all columns do not fit, pan from the team to the two numeric answers. |
| 00:03–00:07 | `START WITH / SIX TICKETS` | Show the six synthetic support tickets in the input result grid. Briefly reveal ticket text so the later choices have context. | Zoom out from results, cut to input, then gently zoom into two short ticket examples. Outline their text cells. |
| 00:07–00:15 | `ONE CALL / THREE ANSWERS` | Show the inference cell's `ai_decide` call, then its three question definitions in sequence: `team` / `choice`, `needs_escalation` / `noul`, `urgency` / `score`. Keep each name paired with its type on screen. | Outline `ai_decide` first. Then move one thin box between the three definitions, reframing each to avoid tiny JSON text. End on the urgency criteria, ordered from 0 to 2. |
| 00:15–00:22 | `ANSWERS / BECOME COLUMNS` | Show the SQL that reads `response.answers`, followed by the result grid. Hold on `escalation_probability` and `urgency_score`. A small supporting label reads `Escalation: 0–1 · Urgency: 0–2`. | Tight crop on the answer extraction, then zoom out to the numeric columns. Outline actual values without implying they are deterministic. |
| 00:22–00:27 | `FILTER AND ROUTE / WITH SQL` | Show the notebook's SQL filter/routing query and then its actual output. Use the recorded result even if a different run selects different rows. | Outline the relevant `WHERE` or `CASE` expression, then move the outline to the matching result column. Keep thresholds exactly as written in the notebook. |
| 00:27–00:32 | `DEPLOY THE DEMO / WITH DABs` | Show the compact job definition in `resources/job.yml`: job key `ai_decide_demo`, task `triage_tickets`, and notebook path `../src/01_ai_decide.py`. | Zoom into the notebook task; outline `notebook_path`. Show only this small configuration area. |
| 00:32–00:35 | `RUN THE JOB` | Show `databricks bundle run ai_decide_demo -t dev -p DEFAULT` in the terminal, then a brief cut to the completed job run. Use terminal line wrapping to keep the command readable in portrait. Prepare the successful run before recording. | Outline `ai_decide_demo` in the command, then the real successful task indicator. Do not imply inference completed in three seconds. |
| 00:35–00:39 | `AI ANSWERS / READY FOR SQL` | Return to the real result grid. Add a small `Beta` label beneath the caption. Hold the final frame. | Gently zoom out; remove the active red outline for a clean ending. |

## Optional continuous voiceover

Databricks ai_decide turns support tickets into structured decisions, directly in SQL. Start with six example tickets. One call chooses a team, estimates escalation probability, and scores urgency. The returned answers become columns you can query. Escalation runs from zero to one; this urgency scale runs from zero to two. Both can be fractional. Now filter and route tickets with normal SQL. The bundle deploys this notebook as a job. Run it and inspect the results. ai_decide is currently in Beta.

## Capture checklist

- Run the notebook successfully before recording; retain the same run's output for the opening, explanation, and ending.
- Record the input grid, inference cell, answer extraction, result grid, routing SQL/output, compact job YAML, command, and successful job run as separate readable captures.
- Treat `noul` as a probability, not a Boolean. Treat this `score` as a weighted average of criterion indices 0–2, not a guaranteed integer or a universal urgency scale.
- Use actual output only. Do not insert invented scores, guarantees of correct classification, or claims about inference speed. Omit waiting time through an obvious cut.
- Check that red boxes track their targets throughout every zoom. Leave a clean hold at the end. Export manually after editing, as usual.
