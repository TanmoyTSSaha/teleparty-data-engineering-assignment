# [PROMPTS.md](http://PROMPTS.md)

All prompts used during this project with **Claude (Cursor Agent)**.

---

## Session 1: System design

**Prompt:**

> @Senior Data Engineering Challenge - PySpark.md Let's design a system for this task.
>
> Do a proper research online first and then share me the designs.
>
> Do not assume anything, ask me questions. Be 100% sure and tech-stack documentation backed. Don't be diplomatic, be realistic.

**Context:** Initial architecture research covering dataset gaps (missing `title.episode.tsv` in Kaggle bundle), OLAP comparison (ClickHouse vs StarRocks vs Doris), Docker/Spark setup, partitioning strategy, and clarifying questions.

---



## Session 2: OLAP comparison and decisions

**Prompt:**

> 1. Let's add the episode tsv
> 2. Will answer it later.
> 3. Let's compare it with StarRocks or other available OLAP which was not compared here.
> 4. I have 16GB Ram system. Need to check docker config.
> 5. Teleparty did not shared anything regarding this. The @Senior Data Engineering Challenge - PySpark.md is the doc shared by them.
> 6. We will decide it once we confirm the olap.
> 7. Yes required. No deadline.
> 8. Let's download it runtime and it does not need API token, as I tested it @kaggle.py
>
> Let's compare more olaps as mentioned.

**Outcome:** StarRocks selected; bronze fallback path `data/bronze/imdb/`; expanded OLAP matrix.

---



## Session 3: Column and analytics scope

**Prompt:**

> @columns.txt:1-47 Let's decide which columns are required for us and what analytics the can perform using the columns we will be ingesting

**Outcome:** Tier 2 column drops, gold table design, Q1–Q6 analytics mapping.

---



## Session 4: Implementation plan

**Prompt:**

> Tier 2 IMDb Lakehouse → StarRocks Pipeline
>
> Implement the plan as specified, it is attached for your reference. Do NOT edit the plan file itself.
>
> To-do's from the plan have already been created. Do not create them again. Mark them as in_progress as you work, starting with the first one. Don't stop until you have completed all the to-dos.

**Outcome:** Full pipeline implementation per attached plan.

---

