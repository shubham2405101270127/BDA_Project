#!/usr/bin/env python3
import os
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import subprocess
import threading
import json

# Safe fallback loading for database drivers
try:
    import pymongo
except ImportError:
    pymongo = None

# Setup environments
PROJECT_ROOT = "C:\\BDA_Project"
os.environ['SPARK_HOME'] = 'C:\\spark'
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['JAVA_HOME'] = 'C:\\PROGRA~1\\ECLIPS~1\\JDK-11~1.6-H'
os.environ['PYSPARK_PYTHON'] = r'C:\Users\shuba\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\shuba\AppData\Local\Programs\Python\Python311\python.exe'

class BDAProjectGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Big Data Analytics - Universal Review Processor")
        
        # FIXED: Removed the broken string that was crashing the layout loop
        self.root.geometry("700x650")
        self.root.configure(bg="#f0f2f5")
        
        # Style Configuration
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Title Label
        title = tk.Label(root, text="Reviews Analytics & NoSQL Serving Pipeline", font=("Helvetica", 16, "bold"), fg="#1a73e8", bg="#f0f2f5")
        title.pack(pady=15)
        
        # File Selection Frame
        frame = tk.Frame(root, bg="#f0f2f5")
        frame.pack(pady=5, fill='x', padx=30)
        
        self.file_label = tk.Label(frame, text="No CSV file selected", font=("Helvetica", 10, "italic"), fg="#5f6368", bg="#f0f2f5", wraplength=450, anchor="w", justify="left")
        self.file_label.pack(side="left", fill='x', expand=True, padx=10)
        
        browse_btn = tk.Button(frame, text="Browse CSV", command=self.browse_file, font=("Helvetica", 10, "bold"), fg="white", bg="#1a73e8", activebackground="#1557b0", relief="flat", padx=10, pady=5)
        browse_btn.pack(side="right")
        
        # Control Buttons Frame
        btn_frame = tk.Frame(root, bg="#f0f2f5")
        btn_frame.pack(pady=15, fill='x', padx=30)
        
        # Run Spark Pipeline Button
        self.run_btn = tk.Button(btn_frame, text="⚡ Run Analytics Engine", command=self.start_processing, font=("Helvetica", 11, "bold"), fg="white", bg="#28a745", activebackground="#218838", relief="flat", pady=8)
        self.run_btn.pack(side="left", fill='x', expand=True, padx=(0, 5))
        self.run_btn.config(state="disabled")
        
        # Fetch directly from MongoDB Button
        self.mongo_btn = tk.Button(btn_frame, text="📊 Fetch MongoDB Serving Layer", command=self.load_from_mongodb, font=("Helvetica", 11, "bold"), fg="white", bg="#6f42c1", activebackground="#5a32a3", relief="flat", pady=8)
        self.mongo_btn.pack(side="right", fill='x', expand=True, padx=(5, 0))
        
        # Terminal/Output Logs Area
        log_label = tk.Label(root, text="Real-time Pipeline Logs & Database Serving Layer Views:", font=("Helvetica", 10, "bold"), fg="#3c4043", bg="#f0f2f5")
        log_label.pack(anchor="w", padx=30, pady=(5, 0))
        
        self.log_text = tk.Text(root, font=("Consolas", 9), bg="#1e1e1e", fg="#d4d4d4", insertbackground="white", wrap="word")
        self.log_text.pack(fill="both", expand=True, padx=30, pady=(5, 20))
        
        self.selected_csv = ""

    def browse_file(self):
        file_path = filedialog.askopenfilename(initialdir="/", title="Select Reviews CSV", filetypes=(("CSV files", "*.csv"), ("all files", "*.*")))
        if file_path:
            self.selected_csv = file_path
            self.file_label.config(text=f"Selected: {os.path.basename(file_path)}", font=("Helvetica", 10, "normal"), fg="#202124")
            self.run_btn.config(state="normal")
            self.log_append(f"[SYSTEM] Local workspace active target updated:\n{file_path}\nReady to process.")

    def log_append(self, message):
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)

    def start_processing(self):
        self.run_btn.config(state="disabled")
        self.mongo_btn.config(state="disabled")
        self.log_text.delete("1.0", tk.END)
        threading.Thread(target=self.execute_pipeline, daemon=True).start()

    def execute_pipeline(self):
        try:
            self.log_append("==============================================")
            self.log_append("🚀 INITIALIZING SPARK ANALYTICS ENGINE...")
            self.log_append("==============================================")
            
            cmd = [
                r"C:\spark\bin\spark-submit.cmd", 
                os.path.join(PROJECT_ROOT, "scripts", "main.py"),
                self.selected_csv
            ]
            
            process = subprocess.Popen(cmd, cwd=PROJECT_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, shell=True)
            
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    if "INFO" not in output:
                        self.log_append(output.strip())
            
            rc = process.poll()
            if rc == 0:
                self.log_append("\n==============================================")
                self.log_append("✅ SUCCESS: DATA INGESTED INTO NOSQL SERVING LAYER!")
                self.log_append("==============================================")
                self.log_append("\n[PRO-TIP]: Click 'Fetch MongoDB Serving Layer' button above to read directly from database!")
            else:
                self.log_append(f"\n❌ Pipeline stopped with exit code: {rc}")
                
        except Exception as e:
            messagebox.showerror("Execution Error", str(e))
        finally:
            self.run_btn.config(state="normal")
            self.mongo_btn.config(state="normal")

    def load_from_mongodb(self):
        self.log_text.delete("1.0", tk.END)
        self.log_append("============================================================")
        self.log_append("🔎 QUERYING LOCAL MONGODB DATABASE LAYER (bda_database)...")
        self.log_append("============================================================\n")
        
        if pymongo is None:
            self.log_append("❌ DATABASE ERROR: 'pymongo' driver is missing.\nRun: pip install pymongo")
            return

        try:
            client = pymongo.MongoClient("mongodb://127.0.0.1:27017/", serverSelectionTimeoutMS=2000)
            db = client["bda_database"]
            client.server_info()
            
            # 1. Inspect Sentiment Analysis Collection
            if "sentiment_analysis" in db.list_collection_names():
                sent_coll = db["sentiment_analysis"]
                total_docs = sent_coll.count_documents({})
                self.log_append(f"[DATABASE METRIC] 'sentiment_analysis' Total Records: {total_docs}\n")
                
                pipeline = [{"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}]
                groups = list(sent_coll.aggregate(pipeline))
                
                self.log_append("--- Aggregated Sentiment Summary ---")
                for group in groups:
                    self.log_append(f" * {group['_id'] or 'Unknown'}: {group['count']} items")
                
                self.log_append("\n--- Database Storage Sample Preview (Top 3 Records) ---")
                samples = sent_coll.find({}, {"review_clean": 1, "Rating": 1, "sentiment": 1}).limit(3)
                for item in samples:
                    clean_dict = {k: v for k, v in item.items() if k != '_id'}
                    self.log_append(str(clean_dict))
            else:
                self.log_append("⚠️ Collection 'sentiment_analysis' not created yet. Run the pipeline first.")

            # 2. Inspect Product Recommendations Collection
            self.log_append("\n" + "-"*60)
            if "recommendations" in db.list_collection_names():
                recs_coll = db["recommendations"]
                total_recs = recs_coll.count_documents({})
                self.log_append(f"[DATABASE METRIC] 'recommendations' Total Users Indexed: {total_recs}\n")
                
                self.log_append("--- Target Product Recommendations Sample View ---")
                samples_recs = recs_coll.find().limit(3)
                for item in samples_recs:
                    user_id = item.get("user_id", "N/A")
                    raw_recs = item.get("recommendations", "[]")
                    
                    try:
                        parsed_recs = json.loads(raw_recs) if isinstance(raw_recs, str) else raw_recs
                        trimmed_recs = parsed_recs[:3]
                    except Exception:
                        trimmed_recs = str(raw_recs)[:80]
                        
                    self.log_append(f" User Profile ID [{user_id}] -> Top Recommendations: {trimmed_recs}...")
            else:
                self.log_append("⚠️ Collection 'recommendations' not loaded yet.")
                
            self.log_append("\n============================================================")
            self.log_append("🏁 END OF DIRECT DATABASE INTERVIEW DISCOVERY VIEW")
            self.log_append("============================================================")
            
        except Exception as e:
            self.log_append(f"❌ DATABASE ERROR: Could not talk to local MongoDB server.\nDetails: {str(e)}")
            self.log_append("\n👉 Make sure your MongoDB Server service is running in Windows task background panels!")

if __name__ == "__main__":
    root = tk.Tk()
    app = BDAProjectGUI(root)
    root.mainloop()
