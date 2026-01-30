from setuptools import find_packages, setup


setup(
    name="polyreputation",
    version="0.1.0",
    description="Polymarket on-chain trades → reputation, tags, leaderboards (SQLite + FastAPI + Streamlit)",
    package_dir={"": "src"},
    packages=find_packages("src"),
    python_requires=">=3.9",
)

