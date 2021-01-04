import time
import os
import sys
import random
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.utils import LRU_Index
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

STAR_WARS_CHARACTERS = [
    "Luke Skywalker",
    "C-3PO",
    "R2-D2",
    "Darth Vader",
    "Leia Organa",
    "Owen Lars",
    "Beru Whitesun Lars",
    "R5-D4",
    "Biggs Darklighter",
    "Obi-Wan Kenobi",
    "Anakin Skywalker",
    "Wilhuff Tarkin"
    "Chewbacca",
    "Han Solo",
    "Greedo",
    "Jabba Desilijic Tiure",
    "Wedge Antilles",
    "Jek Tono Porkins",
    "Yoda",
    "Palpatine",
    "Boba Fett",
    "IG-88",
    "Bossk",
    "Lando Calrissian",
    "Lobot",
    "Ackbar",
    "Mon Mothma",
    "Arvel Crynyd",
    "Wicket Systri Warrick"
    "Nien Nunb",
    "Qui-Gon Jinn",
    "Nute Gunray",
    "Finis Valorum",
    "Jar Jar Binks",
    "Roos Tarpals",
    "Rugor Nass",
    "Ric Olié",
    "Watto",
    "Sebulba",
    "Quarsh Panaka",
    "Shmi Skywalker",
    "Darth Maul",
    "Bib Fortuna",
    "Ayla Secura",
    "Dud Bolt",
    "Gasgano",
    "Ben Quadinaros",
    "Mace Windu",
    "Ki-Adi-Mundi",
    "Kit Fisto",
    "Eeth Koth",
    "Adi Gallia",
    "Saesee Tiin",
    "Yarael Poof",
    "Plo Koon",
    "Mas Amedda",
    "Gregar Typho",
    "Cordé",
    "Cliegg Lars",
    "Poggle the Lesser",
    "Luminara Unduli",
    "Barriss Offee",
    "Dormé",
    "Dooku",
    "Bail Prestor Organa",
    "Jango Fett",
    "Zam Wesell",
    "Dexter Jettster",
    "Lama Su",
    "Taun We",
    "Jocasta Nu",
    "Ratts Tyerell",
    "R4-P17",
    "Wat Tambor",
    "San Hill",
    "Shaak Ti",
    "Grievous",
    "Tarfful",
    "Raymus Antilles",
    "Sly Moore",
    "Tion Medon",
    "Finn",
    "Poe Dameron",
    "Captain Phasma",
    "Padmé Amidala"
]

def lru_performance():

    lru = LRU_Index(size=25)

    values = []
    for i in range(1000):
        values.append(random.choice(STAR_WARS_CHARACTERS))

    start = time.time_ns()

    for i in range(1175):   # 1175 ~ 1 second
        for val in values:
            lru.test(val)

    print((time.time_ns() - start) / 1e9)


if __name__ == "__main__":
    lru_performance()

    print('okay')
