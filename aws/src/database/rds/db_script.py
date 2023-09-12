from aws.src.database.rds.uh_mirror.entities.PropertyAlertNew import PropertyAlertNew
from enums.enums import Stage
from uh_mirror.session_for_uh_mirror import session_for_uh_mirror

STAGE = Stage.BASE_STAGING


def main():
    UhSession = session_for_uh_mirror(STAGE)
    with UhSession.begin() as session:
        alerts = session.query(PropertyAlertNew) \
            .where(PropertyAlertNew.property_reference == "00005793") \
            .limit(10) \
            .all()
        print("\n\n=====================\n\n")

        if len(alerts) == 0:
            print("No alerts found")

        for alert in alerts:
            print(alert.person_name, alert.address)


if __name__ == "__main__":
    main()
