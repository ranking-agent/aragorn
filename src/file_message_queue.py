import asyncio
import os
import time
import aiofiles
import logging
logger = logging.getLogger(__name__)


class FileMessageQueue:
    def __init__(self):
        self.response_dir = os.environ.get('FILE_RESPONSE_PATH', 'responses/')
        self.signals_dir = os.environ.get('SIGNALS_PATH', 'signals/')
        self.make_dirs_if_not_exist(self.response_dir)
        self.make_dirs_if_not_exist(self.signals_dir)
        self.delete_files = not os.environ.get('KEEP_FILES_IN_QUEUE')

    def make_dirs_if_not_exist(self, dir_name):
        os.makedirs(dir_name, exist_ok=True)

    async def subscribe(self, guid, time_out=60 * 60 * 4):
        timed_out = False
        is_done = False
        start = time.time()
        while not is_done and not timed_out:
            try:
                async with aiofiles.open(os.path.join(self.signals_dir, guid), 'r') as stream:
                    is_done = True
            except:
                # just keep on waiting if file is not found till we time out
                await asyncio.sleep(0.1)
            timed_out = (time_out < (time.time() - start))
        if timed_out:
            error_string = f'{guid}: Waiting for {guid} response file timed out.'
            logger.error(error_string)
            raise asyncio.exceptions.TimeoutError(error_string)
        else:
            async with aiofiles.open(os.path.join(self.response_dir,f'{guid}.json'), 'r') as stream:
                data = await stream.read()
                return data

    async def publish(self, guid, data):
        async with aiofiles.open(os.path.join(self.response_dir,f'{guid}.json'), 'w') as stream:
            await stream.write(data)
        # once done with the file write a smaller signal file
        # to initiate file reading
        async with aiofiles.open(os.path.join(self.signals_dir, guid), 'w') as stream:
            pass

    def clean_up_files(self, guid):
        if self.delete_files:
            if os.path.exists(os.path.join(self.response_dir, f'{guid}.json')):
                os.remove(os.path.join(self.response_dir, f'{guid}.json'))
            if os.path.exists(os.path.join(self.signals_dir, guid)):
                os.remove(os.path.join(self.signals_dir, guid))
