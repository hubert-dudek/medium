# ai_decide in everyday life — 30-second short

**Idea:** one trip, three simple questions. Show the sentence, the question, and its actual answer. Source: [01_ai_decide.sql](src/01_ai_decide.sql).

## Shots

` / ` means a caption line break. Captions are large red text at the top.

| Time | Exact caption | What the viewer sees | Highlight / camera |
| --- | --- | --- | --- |
| 00:00–00:02 | `AI DECISIONS / IN SQL` | Start with example 1 already in view: the train expense and its result. | Brief red outline around `ai_decide`. Start on useful content, with no title-only screen. |
| 00:02–00:09 | `SORT MY / TRAVEL EXPENSE` | The sentence `Train from Prague to Vienna, EUR 18.` Then the three category choices, followed by the actual `expense_category` result. Small label: `choice`. | Zoom into the sentence for 2 seconds; shift the box to the category choices for 2 seconds; pan down to the one-cell answer and hold for 3 seconds. |
| 00:09–00:17 | `CAN I BRING / MY DOG?` | Example 2: `Small dogs are welcome for EUR 15 per night.` Show the question, then the actual `dogs_allowed_probability`. Small label beside the result: `Probability · 0–1`. | Zoom out before cutting to the next cell. Outline the policy sentence, then the question, then the numeric answer. Hold on the result for 3 seconds. |
| 00:17–00:26 | `WAS THE / GUEST HAPPY?` | Example 3: `Great location, but a noisy room and cold breakfast.` Show `Unhappy`, `Mixed feelings`, `Happy`, then the actual satisfaction score. Small label: `0 = unhappy · 1 = mixed · 2 = happy`. | Outline the review. Pan to the three criteria, then the result. Keep the scale visible while holding on the score for 3 seconds. |
| 00:26–00:30 | `CHOOSE. / CHECK. SCORE.` | Three compact result cards, using the actual recorded outputs: `Expense`, `Dogs allowed (0–1)`, `Guest satisfaction (0–2)`. Small footer: `Databricks ai_decide · Beta`. | Gentle zoom out, then hold the final frame. |

## Optional voiceover

AI decisions, straight from SQL. A train expense: transport, hotel, or food? Let ai_decide choose. Can I bring my dog? Ask it to read the hotel policy and return a probability. Was this guest happy? Score the review from unhappy to happy, even when the feedback is mixed. Three everyday questions. Three tiny SQL queries.

## Style and recording

- Match `v6.png`: white background, large pure-red `#FF0000` top captions, thin unfilled red boxes, and occasional red arrows.
- Use 1080 × 1920 for Shorts. Reserve the top quarter for captions. For square LinkedIn video, reframe the same shots with a shorter caption band.
- Frame one sentence, one question, or one result at a time. Do not squeeze the whole notebook onto a portrait canvas. Keep zooms short and hold still while the viewer reads.
- Run all three cells before recording. Reuse those outputs in the final cards. Keep the actual probabilities and scores; no invented values or binary replacement for a probability.
- Cut execution waits. No setup footage, deployment terminal, or job diagram. The bundle is provided with the demo code.
- Finish on the result cards and leave export to the user.
