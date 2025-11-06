from .task import Task
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

from pathlib import Path


class FileTask(Task):
    def __init__(self, *args):
        super().__init__(*args)


class LoadFileTask(FileTask):
    def run(self):

        fn = self.args.get('fn')
        path = self.context['data_in_path'].joinpath(fn)

        if not path.exists():
            logger.error("File does not exist: %s", path)
            return

        try:
            with path.open('r') as f:
                content = f.read()
            provides = self.provides[0]['key']
            self.context[provides] = content
            logger.info(f"Loaded file {path} into context under key '{provides}'")
        except Exception as e:
            logger.exception("Failed to load file: %s", e)


class CopyFileTask(FileTask):
    def run(self):
        import shutil
        from pathlib import Path

        src = self.args.get('src')
        dst = self.args.get('dst')
        if not src or not dst:
            logger.error("CopyFileTask needs src and dst args")
            return
        """
        # Resolve data_path and app_name from context (attr or dict)
        ctx = self.context
        data_path = None
        data_path = self.context.get('data_path')
        logger.debug(f"CopyFileTask using data_path: {data_path}, join {src} -> {dst}")
        # cast to path object if not yet one
        if not isinstance(data_path, Path):
            data_path = Path(data_path)

        app_name = self.context.get('app_name')
        srcpath = data_path.joinpath(src)
        dstpath = data_path.joinpath(dst)
        """
        srcpath = Path(src)
        dstpath = Path(dst)

        try:
            if not srcpath.exists():
                logger.error("Source does not exist: %s", srcpath)
                return

            # Ensure destination directory exists
            dstpath.parent.mkdir(parents=True, exist_ok=True)

            # Atomic copy: copy to a temp file in the destination dir, then replace
            tmp_name = dstpath.name + f'.{os.getpid()}.tmp'
            tmp_path = dstpath.parent.joinpath(tmp_name)
            shutil.copy2(srcpath, tmp_path)
            os.replace(str(tmp_path), str(dstpath))

            logger.info(f"Copied file from {srcpath} to {dstpath}")
        except Exception as e:
            logger.exception("Failed to copy file: %s", e)

