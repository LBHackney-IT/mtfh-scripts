from dataclasses import dataclass
from typing import Optional


@dataclass
class PropertyAlertNew:
    id: int
    alert_id: str
    is_active: bool
    person_name: str
    mmh_id: Optional[str] = None
    door_number: Optional[str] = None
    address: Optional[str] = None
    neighbourhood: Optional[str] = None
    date_of_incident: Optional[str] = None
    code: Optional[str] = None
    caution_on_system: Optional[str] = None
    property_reference: Optional[str] = None
    uprn: Optional[str] = None
    outcome: Optional[str] = None
    assure_reference: Optional[str] = None

    def __repr__(self) -> str:
        return f"PropAlert(id={self.id!r}, alert_id={self.alert_id!r}, fullname={self.person_name!r})"


if __name__ == "__main__":
    print("Hello World!")
    my_prop_alert = PropertyAlertNew(
        id=1,
        door_number="1",
        address="1",
        neighbourhood="1",
        date_of_incident="1",
        code="1",
        caution_on_system="1",
        property_reference="1",
        uprn="1",
        person_name="1",
        mmh_id="1",
        # outcome="1",
        assure_reference="1",
        is_active=True,
        alert_id="1",
    )

    print(my_prop_alert)
