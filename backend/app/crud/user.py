# crud/user.py
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.models.user import User

def get_by_username(db: Session, username: str) -> User | None:
    return db.scalar(select(User).where(User.username == username))

def create(db: Session, username: str) -> User:
    u = User(username=username)
    db.add(u)
    db.commit()
    db.refresh(u)
    return u
