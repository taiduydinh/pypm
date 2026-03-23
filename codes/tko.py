import subprocess
import os
import sys
import re

# ==============================
# CONFIGURATION
# ==============================

JAVA_FOLDER = r"C:\Users\acer\Downloads\Naresh-2\Naresh-2\102_TKO\Java\src"
MAIN_CLASS = "MainTestTKOBasic"
JAVA_FILE = os.path.join(JAVA_FOLDER, "MainTestTKOBasic.java")

# 👇 CHANGE k VALUE HERE
K_VALUE = 7

# ==============================

def update_k_value():
    print(f"Updating k value to {K_VALUE} in Java file...")

    with open(JAVA_FILE, "r") as f:
        content = f.read()

    # Replace line: int k = something;
    new_content = re.sub(r"int k\s*=\s*\d+;", f"int k = {K_VALUE};", content)

    with open(JAVA_FILE, "w") as f:
        f.write(new_content)

    print("k value updated.\n")


def compile_java():
    print("Compiling Java files...")
    result = subprocess.run(
        "javac *.java",
        cwd=JAVA_FOLDER,
        shell=True,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print("Compilation Error:")
        print(result.stderr)
        sys.exit(1)

    print("Compilation successful.\n")


def run_java():
    print("Running TKO (Java)...")

    result = subprocess.run(
        f"java {MAIN_CLASS}",
        cwd=JAVA_FOLDER,
        shell=True,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print("Execution Error:")
        print(result.stderr)
        sys.exit(1)

    print(result.stdout)


def print_output():
    output_path = os.path.join(JAVA_FOLDER, "output.txt")

    print("\n===== TOP-K HIGH UTILITY ITEMSETS =====\n")

    with open(output_path, "r") as f:
        for line in f:
            print(line.strip())


# ==============================

if __name__ == "__main__":
    update_k_value()
    compile_java()
    run_java()
    print_output()