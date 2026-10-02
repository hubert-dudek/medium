# ai_decide: three everyday questions

Three small, independent SQL examples for a short video. Open [the notebook](src/01_ai_decide.sql) and run any example on its own. Each query takes one sentence and displays one answer.

| Scenario | Question type | What you see |
| --- | --- | --- |
| A train expense from Prague to Vienna | `choice` | An expense category: transport, hotel, or food |
| A hotel policy that welcomes small dogs | `noul` | Probability that you can bring your dog, from 0 to 1 |
| A review praising the location but criticizing the room and breakfast | `score` | Guest satisfaction from 0 (unhappy) to 2 (happy) |

`noul` returns a probability. `score` returns a weighted average of the ordered criterion indices, so it can be fractional. The numeric outputs are displayed to two decimal places. All text is fictional; record the actual model outputs, which can vary.

## Run with DABs

Requires a configured Databricks CLI profile, serverless Jobs compute, and access to the `ai_decide` Beta in a supported region.

```bash
cd topics/202610/ai_decide
databricks bundle validate -t dev -p DEFAULT
databricks bundle deploy -t dev -p DEFAULT
databricks bundle run ai_decide_demo -t dev -p DEFAULT
```

Open the `everyday_examples` notebook task to see all three results, or run the deployed notebook interactively for recording. No tables, schemas, data loading, or extra packages are needed. Normal compute and AI function charges apply.

If a result is `NULL`, temporarily remove the part after `ai_decide(...)` beginning with `:response` to inspect the full response and its `error_message`. The short examples intentionally show only the answer.

## Video

Use [SCREENPLAY.md](SCREENPLAY.md): 30 seconds, one everyday question at a time. Record the input, the question, and the result. Deployment stays outside the video.

Locally checked: all three SQL statements and question definitions, the notebook path, and the official bundle schema. Workspace execution has not been performed.

[Databricks ai_decide reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_decide)
