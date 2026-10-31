import os
import tempfile
import unittest
import zipfile

from component import ShopZipWriter


class TestShopZipWriter(unittest.TestCase):

    def test_split_keeps_all_items_per_shop(self):
        with tempfile.TemporaryDirectory() as out:
            with ShopZipWriter(out, 'pricehistory', max_files_per_zip=3) as writer:
                for i in range(7):
                    writer.get_zip('a.cz').writestr(f'a{i}.json', '{}')
                    if i < 4:
                        writer.get_zip('b.sk').writestr(f'b{i}.json', '{}')

            def names(f):
                with zipfile.ZipFile(os.path.join(out, f)) as z:
                    return z.namelist()

            self.assertEqual(sorted(os.listdir(out)), ['a.cz_pricehistory_1.zip', 'a.cz_pricehistory_2.zip',
                                                       'a.cz_pricehistory_3.zip', 'b.sk_pricehistory_1.zip',
                                                       'b.sk_pricehistory_2.zip'])
            self.assertEqual(names('a.cz_pricehistory_1.zip'), ['a0.json', 'a1.json', 'a2.json'])
            self.assertEqual(names('a.cz_pricehistory_2.zip'), ['a3.json', 'a4.json', 'a5.json'])
            self.assertEqual(names('a.cz_pricehistory_3.zip'), ['a6.json'])
            self.assertEqual(names('b.sk_pricehistory_1.zip'), ['b0.json', 'b1.json', 'b2.json'])
            self.assertEqual(names('b.sk_pricehistory_2.zip'), ['b3.json'])


if __name__ == "__main__":
    unittest.main()
