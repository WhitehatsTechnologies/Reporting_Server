# import subprocess


# def install_packages_from_requirements(requirements_file):
#     with open(requirements_file, "r") as file:
#         for line in file:
#             if line.strip() and not line.startswith("#"):
#                 package = line.strip()
#                 install_package(package)


# def install_package(package):
#     try:
#         subprocess.check_call(["pip", "install", "--no-cache-dir", package])
#         print(f"==========> Successfully installed: {package} <==========")
#     except subprocess.CalledProcessError:
#         print(f"==========> Failed to install: {package} <==========")


# if __name__ == "__main__":
#     requirements_file = "requirements.txt"
#     install_packages_from_requirements(requirements_file)

import subprocess
import argparse


def install_packages_from_requirements(requirements_file, fresh=False):
    with open(requirements_file, "r") as file:
        for line in file:
            if line.strip() and not line.startswith("#"):
                package = line.strip()
                install_package(package, fresh)


def install_package(package, fresh):
    cmd = ["pip", "install"]
    
    if fresh:
        cmd += ["--no-cache-dir"]
    
    cmd.append(package)

    try:
        subprocess.check_call(cmd)
        print(f"✅ Successfully installed: {package}")
    except subprocess.CalledProcessError:
        print(f"❌ Failed to install: {package}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fresh", action="store_true", help="Force fresh install (no cache)")
    args = parser.parse_args()

    requirements_file = "requirements.txt"
    install_packages_from_requirements(requirements_file, args.fresh)
