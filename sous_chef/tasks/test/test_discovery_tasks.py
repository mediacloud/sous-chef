import unittest
import datetime as dt

from ..discovery_tasks import _matching_stories


class TestDiscoveryTasks(unittest.TestCase):

    def test_matching_stories(self):
        # trying to recreate a SautiHealth query that resulted in 504 on 29/Jul/2026:23:20:36 -0400
        query = ' ( '\
                '(\n'\
                '  ("World Health Organization" OR "United Nations" OR "Centers for Disease Control and Prevention" OR \n'\
                '  "CDC" OR "UNICEF" OR "MSF" OR "Doctors without Borders" OR "World Bank" OR "Bola Tinubu" OR \n'\
                '  "Emmanuel Macron" OR "Global Fund" OR "Gavi" OR "Nigeria Center for Disease Control and Prevention" OR \n'\
                '  "NCDC" OR "United Nations Population Fund" OR "UNFPA" OR "African Union" OR "USAID" OR \n'\
                '  "Abdelmadjid Abderahim" OR "Adrien Mougougou" OR "Africa Centres for Disease Control and Prevention" OR \n'\
                '  "Africa CDC" OR "African National Congress" OR "ANC" OR "AstraZeneca" OR "Bassirou Diomaye Faye" OR \n'\
                '  "Bavarian Nordic" OR "Bill & Melinda Gates Foundation" OR "Capitaine Ibrahim Traore" OR \n'\
                '  "Catherine Russell" OR "Cyril Ramaphosa" OR "Dabsou Guidaoussou" OR "Denis Sassou" OR \n'\
                '  "Donald Trump" OR "Dr. Matshidiso Moeti" OR "Economic and Financial Crimes Commission" OR \n'\
                '  "Economic Community of West African States" OR "ECOWAS" OR "CEDEAO" OR "European Union" OR \n'\
                '  "Expertise France" OR "Food and Agriculture Organization" OR "FAO" OR "World Economic Forum" OR \n'\
                '  "Foster Mohale" OR "Frédéric Valletoux" OR "Gabriel Attal" OR "Gilbert Mokoki" OR "GSK" OR \n'\
                '  "Jean Kaseya" OR "Jide Idris" OR "Joe Biden" OR "Joe Phaahla" OR "Makerere University" OR \n'\
                '  "Manaouda Malachie" OR "Melinda Gates" OR "Ministry of Health and Child Care" OR "MTN" OR \n'\
                '  "Muhammad Ali Pate" OR "National Institute for Communicable Diseases" OR \n'\
                '  "National Primary Health Care Development Agency" OR "NPHCDA" OR "UNAIDS" OR "Ousmane Sonko" OR \n'\
                '  "World Food Program" OR "Pfizer" OR "Pierre Dimba" OR "UNDP" OR "Rotary International" OR \n'\
                '  "Susan Nakhumicha" OR "Tedros Adhanom Ghebreyesus" OR "Tunji Alausa" OR \n'\
                '  "U.S. President\'s Emergency Plan for AIDS Relief" OR "PEPFAR" OR "William Ruto" OR \n'\
                '  "Yoweri Museveni") AND language:en) \n'\
                'AND \n'\
                '  ("Bola Tinubu" AND ("Tinubu said"~10 OR "Tinubu saying"~10 OR "Tinubu stated"~10 OR "Tinubu stating"~10 OR \n'\
                '  "Tinubu warned"~10 OR "Tinubu confirmed"~10 OR "Tinubu announced"~10 OR "Tinubu explained"~10 OR \n'\
                '  "Tinubu added"~10 OR "Tinubu claimed"~10 OR "Tinubu told"~10 OR "Tinubu wrote"~10 OR "Tinubu shared"~10 OR \n'\
                '  "Tinubu recalled"~10 OR "Tinubu described"~10 OR "Tinubu observed"~10 or "Tinubu declared"~10 OR \n'\
                '  "Tinubu disclosed"~10 OR "Tinubu revealed"~10 OR "Tinubu testified"~10 OR "Tinubu affirmed"~10 OR \n'\
                '  "Tinubu asserted"~10 OR "Tinubu argued"~10 OR "Tinubu suggested"~10)\n'\
                ')\n'\
                ')'
        start_date = dt.date(2026, 7, 28)
        end_date = dt.date(2026, 7, 29)
        collection_ids = [
                34411588, 34412034, 34412038, 34412042, 34412047, 34412053, 34412085, 34412093,
                34412125, 34412126, 34412134, 34412155, 34412170, 34412173, 34412177, 34412192,
                34412202, 34412215, 34412222, 34412237, 34412238, 34412248, 34412251, 34412253,
                34412263, 34412281, 34412286, 34412308, 34412312, 34412317, 34412321, 34412330,
                34412348, 34412350, 34412370, 34412379, 34412396, 34412402, 34412406, 34412418,
                34412439, 34412470, 34412471, 38002029, 38376341, 38376358, 38379206, 38379247,
                38379371, 38379378, 38379380, 38379387, 38379389, 38379433, 38379436, 38379588,
                38379804, 38379807, 38379885, 38379888, 38380260, 38380271, 38380274, 38380276,
                38380279, 38380289, 38380292, 38380309, 38380314, 38380317, 38380422, 38380425,
                38380435, 38380500, 38380785, 38380807, 38380809, 38380857, 38380880, 38380886,
                38380907, 38381011, 38381033, 38381096, 38381675, 38381682, 38379591, 38379594,
                38379773 ]
        max_articles = 1000
        stories_df = _matching_stories(query, start_date, end_date, collection_ids,
                                       [], False, max_articles)
        self.assertGreater(len(stories_df.index), 0)


if __name__ == "__main__":
    unittest.main()
