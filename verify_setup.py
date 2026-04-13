import os
import sys

def verify():
    print("--- JobBot Phase 0 Verification ---")
    
    # Check Directories
    dirs = ["logs", "output", "modules", "tests"]
    print("\n[1] Checking Directories:")
    for d in dirs:
        if os.path.isdir(d):
            print(f"  [OK] {d}/ exists")
        else:
            print(f"  [FAIL] {d}/ is missing")

    # Check Files
    files = [
        "requirements.txt", "config.yaml", ".env.example", ".gitignore", "README.md",
        "config.py", "main.py", "modules/__init__.py", "tests/__init__.py"
    ]
    print("\n[2] Checking Core Files:")
    for f in files:
        if os.path.isfile(f):
            print(f"  [OK] {f} exists")
        else:
            print(f"  [FAIL] {f} is missing")

    # Check config.py loading
    print("\n[3] Verifying config.py loading logic:")
    try:
        from config import get_config
        cfg = get_config()
        print("  [OK] Config loaded and validated successfully")
    except Exception as e:
        print(f"  [FAIL] Config loading failed: {e}")

    print("\nVerification Complete.")

if __name__ == "__main__":
    verify()
