import subprocess
import os
import sys
import shutil
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
K_VALUE = 7   # 👈 CHANGE K HERE

def clean_previous_build():
    pkg_dir = os.path.join(BASE_DIR, "ca")
    if os.path.exists(pkg_dir):
        shutil.rmtree(pkg_dir)

    for file in os.listdir(BASE_DIR):
        if file.endswith(".class"):
            os.remove(os.path.join(BASE_DIR, file))

def create_temp_memorylogger():
    original_path = os.path.join(BASE_DIR, "MemoryLogger.java")
    package_dir = os.path.join(BASE_DIR, "ca", "pfv", "spmf", "tools")
    os.makedirs(package_dir, exist_ok=True)

    temp_path = os.path.join(package_dir, "MemoryLogger.java")

    with open(original_path, "r") as f:
        content = f.read()

    package_line = "package ca.pfv.spmf.tools;\n\n"

    with open(temp_path, "w") as f:
        f.write(package_line + content)

def update_k_in_java():
    java_file = os.path.join(BASE_DIR, "MainTestTKUCE.java")

    with open(java_file, "r") as f:
        content = f.read()

    # Replace the line: int k = something;
    new_content = re.sub(r'int k\s*=\s*\d+\s*;', f'int k = {K_VALUE};', content)

    with open(java_file, "w") as f:
        f.write(new_content)

def compile_java():
    print("Cleaning previous build...")
    clean_previous_build()

    print("Updating K in Java file...")
    update_k_in_java()

    print("Preparing package structure...")
    create_temp_memorylogger()

    print("Compiling Java files...")

    compile_cmd = (
        "javac -d . "
        "ca/pfv/spmf/tools/MemoryLogger.java "
        "AlgoTKUCE.java "
        "MainTestTKUCE.java"
    )

    result = subprocess.run(
        compile_cmd,
        cwd=BASE_DIR,
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
    print("Running TKU-CE (Java)...\n")

    result = subprocess.run(
        "java MainTestTKUCE",
        cwd=BASE_DIR,
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
    output_path = os.path.join(BASE_DIR, "output.txt")

    print("===== TOP-K HIGH UTILITY ITEMSETS =====\n")

    with open(output_path, "r") as f:
        for line in f:
            print(line.strip())

if __name__ == "__main__":
    compile_java()
    run_java()
    print_output()