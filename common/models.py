from dataclasses import dataclass, fields
from datetime import date
from enum import Enum


@dataclass
class Person:
    person_id: int
    first_name: str
    birthday: date

    def compare(self, other: 'Person') -> list[str]:
        """Compare current object with the other."""

        result = []
        for field in fields(self):
            if getattr(self, field.name) != getattr(other, field.name):
                result.append(field.name)
        return result

    def get_fields(self) -> tuple[str]:
        """Get all field names from current object."""
        return tuple(field.name for field in fields(self))


class PersonField(Enum):
    person_id = 0
    first_name = 1
    birthday = 2


@dataclass
class BetterPerson(Person):
    birthplace: str
    family_name: str = ''
    occupation: str = ''
    hobby: str = ''
