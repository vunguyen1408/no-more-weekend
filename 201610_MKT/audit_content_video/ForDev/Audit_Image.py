from abc import ABCMeta, abstractmethod
class Audit_Image(object):
    """A image for ads


    Attributes:
        type: net / local
        url:
        hash_md5:
        gg_labels:  lables from gg api
            ['label1','label2']
        local_labels: lables from local
            ['label1','label2']
        gg_texts: texts from gg api
            [   {'text':'text',
                'bouding':'()()()()'
                }
            ]
        local_text: texts from local
            [   {'text':'text',
                'bouding':'()()()()'
                }
            ]
        ads: # ads from gg / fb
            {
                'type':'google/fb',
                'id':00000000
            }
        video: video_id and index
            {   'video_id':000000,
                'video_index':1
            }





    """

    __metaclass__ = ABCMeta

    base_sale_price = 0
    wheels = 0

    def __init__(self, source, url, hash_md5, model, year, sold_on):
        self.source=source
        self.date=date

        self.name = name
        self.url = url
        self.path = 

        self.hash_md5=hash_md5
        self.gg_labels=gg_labels
        self.local_labels: local_labels
        self.gg_texts: gg_texts
        self.local_texts: local_texts
        self.ads:ads
        self.video: video


    def sale_price(self):
        """Return the sale price for this vehicle as a float amount."""
        if self.sold_on is not None:
            return 0.0  # Already sold
        return 5000.0 * self.wheels

    def purchase_price(self):
        """Return the price for which we would pay to purchase the vehicle."""
        if self.sold_on is None:
            return 0.0  # Not yet sold
        return self.base_sale_price - (.10 * self.miles)

    @abstractmethod
    def vehicle_type(self):
        """"Return a string representing the type of vehicle this is."""
        pass
