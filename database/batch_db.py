import pymongo
from info import DATABASE_URI, DATABASE_NAME
import logging
from marshmallow.exceptions import ValidationError

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

myclient = pymongo.MongoClient(DATABASE_URI)
mydb = myclient[DATABASE_NAME]


async def save_file(grp_id, unique_id, file_id, file_ref, caption):
    """Save batch file in database"""
    mycol = mydb[str(grp_id)]
    data = {
        '_id': str(unique_id),
        'file_id': str(file_id),
        'file_ref': str(file_ref),
        'caption': str(caption)
    }

    try:
        mycol.update_one({'_id': str(unique_id)}, {"$set": data}, upsert=True)
        return True, 1
    except ValidationError:
        logger.exception('Error occurred while saving file in database')
        return False, 2
    except:
        logger.exception('Some Error Occured!', exc_info=True)
        return False, 0


async def get_batch(grp_id, unique_id):
    mycol = mydb[str(grp_id)]
    query = unique_id.strip()

    query = mycol.find({'_id': query})

    try:
        for file in query:
            unique_id = file['_id']
            file_id = file['file_id']
            file_ref = file['file_ref']
            caption = file['caption']
            try:
                alert = file['alert']
            except:
                alert = None
        return unique_id, file_id, file_ref, caption
    except:
        return None, None, None, None
