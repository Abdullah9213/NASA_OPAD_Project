# Project Report: End-to-End MLOps Pipeline for NASA APOD

**Author:** Abdullah (22i-2515)  
**Date:** 2025-11-16  
**Platform:** Astronomer (Astro) Cloud  
**Status:** Successful

**Executive Summary**
- **Summary:** Automated daily MLOps pipeline that fetches NASA APOD, stores it in Supabase Postgres, and versions data with DVC and GitHub. Deployed and stabilized on Astro Cloud (Dag Version v4).

**Project Objective**
- **Goal:** Daily automated ETL + data & code versioning for NASA APOD.
- **Steps:** Extract → Transform → Load (Supabase & CSV) → DVC add → Git commit & push.

**Architecture**
- **Orchestration:** Apache Airflow (Astro Cloud) DAG (v4).  
- **Integrations:** NASA APOD API, Supabase Postgres, DVC, GitHub.

**DAG Tasks**
- **extract_apod_data:** GET NASA APOD API; returns JSON via XCom.  
- **transform_apod_data:** Select fields (date, title, url, explanation), build single-row pandas.DataFrame.  
- **load_to_postgres:** Upsert into Supabase `apod_data` table (date primary key).  
- **load_to_csv:** Save to `data/apod_data.csv` on worker filesystem.  
- **version_data_and_code:** Idempotent Git + DVC workflow: sync remote, `dvc add`, `git add/commit/push`.

**Deployment Notes & Debugging**
- **Networking:** Resolved IPv6/IPv4 incompatibility by using Supabase connection pooler IPv4 endpoint.  
- **Git State Handling:** Final robust approach: force-sync worker with `git fetch` + `git reset --hard origin/main` (handles untracked/stale state).

**Evidence**
Below are placeholders for images that demonstrate successful runs and visibility. Place images in `images/` and update paths as needed.

- Image 1 — Successful DAG Run:
<img width="464" height="214" alt="{808ED626-63F6-41B5-AAEC-71C908D43B36}" src="https://github.com/user-attachments/assets/ba3531f5-3e7e-44f5-8d16-0ee990f6e8bb" />

- Image 2 — Automated GitHub Commits:
<img width="464" height="185" alt="{16161840-EFD4-4E36-B8E2-4AC5ED985E33}" src="https://github.com/user-attachments/assets/78c9689e-bff8-44bc-9a96-ff6d9e7e055b" />

- Image 3 — Astro Cloud Deployments:
<img width="465" height="203" alt="{F13C4CBF-D060-4854-B0F1-288F55497C39}" src="https://github.com/user-attachments/assets/058f3fc5-5a8f-4d09-a8cd-b58af555f0a0" />


**Quick Usage**
- **Credentials:** Store `GITHUB_PAT`, Supabase credentials, and other secrets as Airflow Variables/Connections in Astro.  
- **File:** The DAG and scripts are expected under the project repo; the CSV output is `data/apod_data.csv`.  
- **Versioning Flow:** The DAG's final task runs `dvc add data/apod_data.csv` then commits the `.dvc` file and pushes to `origin/main`.

**Key Learnings**
- Git in ephemeral workers is fragile — prefer force-sync from remote.  
- Verify network stack (IPv4/IPv6) between managed services.  
- Iterative debugging and test-deploy cycles are essential.

**Links & Access**
- **Airflow UI:** (private to Astro workspace) Provide Astro workspace access to view.  
- **Astro Deployment:** (private) Provide deployment URL to authorized users.

**Contact**
- **Author:** Abdullah — for access requests or questions.

