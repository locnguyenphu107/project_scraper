import os
import pandas as pd
import json
import time
import datetime
import signal
from dotenv import load_dotenv
import google.generativeai as genai

# Load API key from .env file
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    raise ValueError("❌ GEMINI_API_KEY not found. Please set it in .env file.")
genai.configure(api_key=API_KEY)

# Gemini model
MODEL = "gemini-2.5-flash"

def classify_titles(titles):
    """
    Send a bulk list of titles to Gemini and classify them.
    Returns a dictionary {title: {"tier": ..., "relevant": ...}}
    """
    prompt = f"""
    You are helping PostCo (a returns and resale automation SaaS for ecommerce brands) to identify the most relevant job titles for outreach to sell PostCo’s return and resale automation platform.  
    The goal is to focus on ecommerce, decision-makers and operational/customer service leaders, while excluding unrelated functions.

    Classify each job title into one of:

    - Tier 1: Ecommerce related  
    Includes: Ecommerce Manager, Head of Ecommerce, Shopify roles, Digital, Growth, Performance Marketing, CRM, Retention, Email Marketing (only if ecommerce-related).  
    Focus: Roles directly responsible for ecommerce strategy, digital sales channels, online growth, and customer lifecycle.  

    - Tier 2: Decision Maker  
    Includes: CEO, Founder, Co-Founder, Owner, President, Managing Director, Vice President (VP standalone).  
    Rules:  
    - Always prioritize these over other titles if combined.  
    - If the VP title specifies a department/function (e.g., "VP of Sales", "VP of Marketing", "VP of Design", "CTO"):  
    - Tier 1 if Ecommerce/Digital/Growth/CRM.  
    - Tier 3 if Ops/CS/CX.  
    - Not Relevant otherwise.  
    - Prioritize higher authority in multi-title roles (e.g., "Founder & CTO" → Tier 2).  

    - Tier 3: Customer Service / Operations  
    Includes: Operations Manager, Head of Operations, Customer Success, Customer Service, CS, CX, Ops, Logistics, Supply Chain (only if related to ecommerce fulfillment/returns).  
    Focus: Roles overseeing customer experience, returns handling, warehouse operations, or post-purchase processes.  

    - Tier 4: General Management  
    Includes: General Manager, Manager, Director, Head, Lead, or C-level executives not already covered by Tier 1–3.  
    Condition: Only if the role has broad management authority related to business, brand, or strategy.  
    Exclude roles primarily in technical, creative, or administrative domains.  

    - Not Relevant: Everything else unrelated to ecommerce, decision-making, or post-purchase operations. Explicitly exclude:  
    - Technology/IT/Engineering (CTO, VP of Engineering, Software Manager, Data/Analytics, Information Systems)  
    - Design/Creative/Brand/Content/Product-only (Creative Director, Designer, Art Director, Copywriter, Events/Entertainment)  
    - Manufacturing/Production (Factory Manager, Plant Director, Sourcing, Product Development)  
    - Finance (CFO, VP of Finance, Controller, Accountant, FP&A, Payroll)  
    - HR, Legal, Admin, Training, Education, Procurement  
    - Assistants, Coordinators, Interns, Analysts (unless ecommerce-specific)  
    - Sales roles without explicit ecommerce responsibility (e.g., Sales Associate, Account Executive, Business Development, VP of Sales)  

    Multi-title handling:  
    - If a job title contains multiple roles (e.g., "Founder & CTO", "CEO and VP of Sales"), always select the highest-priority tier:  
    Priority order: **Tier 2 > Tier 1 > Tier 3 > Tier 4 > Not Relevant**.  
    Examples:  
    - "Founder & CTO" → Tier 2 (Founder dominates).  
    - "CEO and VP of Sales" → Tier 2 (CEO dominates).  
    - "Head of Ecommerce & Operations Manager" → Tier 1 (Ecommerce dominates Ops).  
    - "General Manager & Creative Director" → Tier 4 (General Manager dominates Creative).  

    Return valid JSON strictly in this format:
    {{
    "Job Title": {{"tier": "Tier X" or "Not Relevant", "relevant": "Yes" or "No"}},
    ...
    }}

    Here are the titles:
    {titles}
    """

    response = genai.GenerativeModel(MODEL).generate_content(prompt)

    try:
        result_text = response.text.strip()
        if result_text.startswith("```"):
            result_text = result_text.split("```")[1].replace("json", "").strip()
        data = json.loads(result_text)
    except Exception as e:
        print("⚠️ Parsing error:", e)
        data = {}

    return data

def process_excel(input_path, file_name, output_path=None, title_col="Title"):
    input_file = os.path.join(input_path, file_name)
    df = pd.read_excel(input_file)

    results = {}
    batch_size = 50
    titles = df[title_col].dropna().unique().tolist()

    processed_count = 0
    total_count = len(titles)

    # Handle interruptions → save partial results
    def save_partial(signum=None, frame=None):
        print("\n💾 Saving partial progress due to interruption...")
        save_results(df, results, input_path, file_name, output_path, partial=True, title_col=title_col)
        exit(0)

    signal.signal(signal.SIGINT, save_partial)
    signal.signal(signal.SIGTERM, save_partial)

    for i in range(0, len(titles), batch_size):
        batch = titles[i:i+batch_size]
        classified = classify_titles(batch)

        # Update results and log each title
        for t in batch:
            if t in classified:
                tier = classified[t]["tier"]
                relevant = classified[t]["relevant"]
                print(f"   → {t} → {tier}, Relevant: {relevant}")
            else:
                print(f"   ⚠️ {t} → Not returned by model")
            results[t] = classified.get(t, {"tier": "Not Relevant", "relevant": "No"})

        processed_count += len(batch)
        print(f"\n✅ Processed {processed_count}/{total_count} titles so far...\n")
        time.sleep(2)  # avoid rate limits

    # Final save
    save_results(df, results, input_path, file_name, output_path, partial=False, title_col=title_col)
    print("🎉 All titles processed and file saved.")

def save_results(df, results, input_path, file_name, output_path=None, partial=False, title_col="Title"):
    """Helper to save DataFrame with current classification results."""
    df["Tier"] = df[title_col].apply(lambda x: results.get(x, {}).get("tier", "Not Relevant"))
    df["Relevant"] = df[title_col].apply(lambda x: results.get(x, {}).get("relevant", "No"))

    if not output_path:
        output_path = input_path
    base_name, ext = os.path.splitext(file_name)

    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if partial:
        output_file = os.path.join(output_path, f"{base_name}_title_checked_partial_{current_time}.xlsx")
    else:
        output_file = os.path.join(output_path, f"{base_name}_title_checked_{current_time}.xlsx")

    df.to_excel(output_file, index=False)
    if partial:
        print(f"💾 Partial progress saved to {output_file}")
    else:
        print(f"✅ Final processed file saved to {output_file}")

if __name__ == "__main__":
    input_path = input("Enter input path directory: ").strip()
    file_name = input("Enter Excel file name (e.g. people.xlsx): ").strip()
    output_path = input("Enter output path directory (press Enter to use input path): ").strip()
    title_col = input("Enter column name for job titles (default: Title): ").strip() or "Title"

    if not output_path:
        output_path = None  # fallback to input path

    process_excel(input_path, file_name, output_path, title_col=title_col)
