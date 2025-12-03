import pandas as pd
import re


def clean_patch(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
  
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    
    s = re.sub(r"[^\x09\x0A\x20-\x7E\u0080-\uFFFF]", "", s)
   
    s = s.replace('"', "'")
    return s



def task_1():
    print("Running Task-1...")
    df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_pull_request.parquet")

    out = df[["title", "id", "agent", "body", "repo_id", "repo_url"]].rename(
        columns={
            "title": "TITLE",
            "id": "ID",
            "agent": "AGENTNAME",
            "body": "BODYSTRING",
            "repo_id": "REPOID",
            "repo_url": "REPOURL",
        }
    )

    out.to_csv("task1_all_pull_request.csv", index=False, encoding="utf-8")
    print("✔ Task-1 done → task1_all_pull_request.csv")



def task_2():
    print("Running Task-2...")
    df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_repository.parquet")

    out = df[["id", "language", "stars", "url"]].rename(
        columns={
            "id": "REPOID",
            "language": "LANG",
            "stars": "STARS",
            "url": "REPOURL",
        }
    )

    out.to_csv("task2_all_repository.csv", index=False, encoding="utf-8")
    print("✔ Task-2 done → task2_all_repository.csv")



def task_3():
    print("Running Task-3...")
    df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_task_type.parquet")

    out = df[["id", "title", "reason", "type", "confidence"]].rename(
        columns={
            "id": "PRID",
            "title": "PRTITLE",
            "reason": "PRREASON",
            "type": "PRTYPE",
            "confidence": "CONFIDENCE",
        }
    )

    out.to_csv("task3_pr_task_type.csv", index=False, encoding="utf-8")
    print("✔ Task-3 done → task3_pr_task_type.csv")


def task_4():
    print("Running Task-4...")
    df = pd.read_parquet(
        "hf://datasets/hao-li/AIDev/pr_commit_details.parquet",
        columns=[
            "pr_id",
            "sha",
            "message",
            "filename",
            "status",
            "additions",
            "deletions",
            "changes",
            "patch",
        ],
    )

    df["patch_clean"] = df["patch"].apply(clean_patch)

    out = df.rename(
        columns={
            "pr_id": "PRID",
            "sha": "PRSHA",
            "message": "PRCOMMITMESSAGE",
            "filename": "PRFILE",
            "status": "PRSTATUS",
            "additions": "PRADDS",
            "deletions": "PRDELSS",
            "changes": "PRCHANGECOUNT",
            "patch_clean": "PRDIFF",
        }
    )[
        [
            "PRID",
            "PRSHA",
            "PRCOMMITMESSAGE",
            "PRFILE",
            "PRSTATUS",
            "PRADDS",
            "PRDELSS",
            "PRCHANGECOUNT",
            "PRDIFF",
        ]
    ]

    out.to_csv("task4_pr_commit_details.csv", index=False, encoding="utf-8")
    print("✔ Task-4 done → task4_pr_commit_details.csv")


def task_5():
    print("Running Task-5...")

    
    pr = pd.read_parquet(
        "hf://datasets/hao-li/AIDev/all_pull_request.parquet",
        columns=["id", "agent", "title", "body"],
    )
    ptype = pd.read_parquet(
        "hf://datasets/hao-li/AIDev/pr_task_type.parquet",
        columns=["id", "type", "confidence"],
    )

    pr = pr.rename(
        columns={"id": "ID", "agent": "AGENT", "title": "TITLE", "body": "BODY"}
    )
    ptype = ptype.rename(
        columns={"id": "ID", "type": "TYPE", "confidence": "CONFIDENCE"}
    )

   
    merged = pr.merge(ptype, on="ID", how="left")

    
    keywords = [
        "security",
        "vuln",
        "vulnerability",
        "vulnerable",
        "auth",
        "authentication",
        "authorization",
        "xss",
        "sql injection",
        "cve",
        "secret",
        "key leak",
        "credential",
        "token",
        "csrf",
        "encryption",
        "encrypt",
        "decrypt",
        "ssl",
        "tls",
        "certificate",
        "backdoor",
        "exploit",
        "privilege escalation",
        "privilege",
        "permissions",
        "access control",
        "sanitize",
        "sanitization",
        "injection",
        "hashing",
        "oauth",
        "jwt",
        "harden",
        "hardening",
        "secure",
        "insecure",
        "audit",
        "confidentiality",
        "integrity",
        "availability",
    ]

    
    kw_pattern = re.compile(
        "|".join(re.escape(k) for k in sorted(keywords, key=lambda x: -len(x))),
        flags=re.IGNORECASE,
    )

    def check_security(title, body):
        text = (title or "") + " " + (body or "")
        return 1 if kw_pattern.search(text) else 0

    merged["SECURITY"] = merged.apply(
        lambda r: check_security(r["TITLE"], r["BODY"]), axis=1
    )

    final = merged[["ID", "AGENT", "TYPE", "CONFIDENCE", "SECURITY"]]

    final.to_csv("task5_PR_security_summary.csv", index=False, encoding="utf-8")
    print("✔ Task-5 done → task5_PR_security_summary.csv")



if __name__ == "__main__":
    task_1()
    task_2()
    task_3()
    task_4()
    task_5()
    print("\nAll tasks complete! CSV files are ready.")
