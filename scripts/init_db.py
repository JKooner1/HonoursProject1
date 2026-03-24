from app.db.base import Base
from app.db.session import engine
from app.utils.paths import ensure_project_dirs


def main() -> None:
    ensure_project_dirs()
    Base.metadata.create_all(bind=engine)
    print("Database initialised successfully.")


if __name__ == "__main__":
    main() 