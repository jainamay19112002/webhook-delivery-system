from sqlmodel import Session, select
from app.database import engine, init_db
from app.models import Subscriber

def seed_default_subscribers():
    init_db()
    with Session(engine) as session:
        existing = session.exec(select(Subscriber)).all()
        if not existing:
            sub1 = Subscriber(
                name="Stripe Payment Processor",
                target_url="http://localhost:8000/api/v1/mock/webhook-receiver",
                secret="whsec_stripe_live_demo_987654321",
                event_types="*"
            )
            sub2 = Subscriber(
                name="GitHub Deployment Receiver",
                target_url="http://localhost:8000/api/v1/mock/webhook-receiver",
                secret="whsec_github_live_demo_123456789",
                event_types="payment.succeeded,order.created"
            )
            session.add(sub1)
            session.add(sub2)
            session.commit()
            print("Successfully seeded 2 default test subscribers!")
        else:
            print("Subscribers already exist in DB.")

if __name__ == "__main__":
    seed_default_subscribers()
