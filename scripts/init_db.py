from app.utils.paths import ensure_project_dirs


def main() -> None:
    ensure_project_dirs()
    print("Project directories ready. Database setup will be added next.")


if __name__ == "__main__":
    main()