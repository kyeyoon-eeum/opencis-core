"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

# Restored accelerator application logic in synchronous (threaded) form

import glob
import json
import math
import os
import shutil
import threading
from io import BytesIO
from pathlib import Path
from typing import Optional, cast

from opencis.util.logger import logger
from opencis.util.component import RunnableComponent
from opencis.cxl.component.irq_manager import Irq, IrqManager
from opencis.cxl.component.switch_connection_client import SwitchConnectionClient
from opencis.cxl.component.common import CXL_COMPONENT_TYPE
from opencis.cxl.device.cxl_type1_device import CxlType1Device, CxlType1DeviceConfig
from opencis.cxl.device.cxl_type2_device import CxlType2Device, CxlType2DeviceConfig


def _lazy_import_ml():
    import torch  # noqa: F401
    from PIL import Image  # noqa: F401
    from torch import nn  # noqa: F401
    from torch.utils.data import DataLoader  # noqa: F401
    from torchvision import transforms, datasets  # noqa: F401
    from torchvision.models import efficientnet_v2_s, EfficientNet_V2_S_Weights  # noqa: F401
    from torchinfo import summary  # noqa: F401
    from tqdm.auto import tqdm  # noqa: F401

    return {
        "torch": __import__("torch"),
        "nn": __import__("torch").nn,
        "DataLoader": __import__("torch.utils.data", fromlist=["DataLoader"]).DataLoader,
        "transforms": __import__("torchvision", fromlist=["transforms"]).transforms,
        "datasets": __import__("torchvision", fromlist=["datasets"]).datasets,
        "efficientnet_v2_s": __import__(
            "torchvision.models", fromlist=["efficientnet_v2_s"]
        ).efficientnet_v2_s,
        "EfficientNet_V2_S_Weights": __import__(
            "torchvision.models", fromlist=["EfficientNet_V2_S_Weights"]
        ).EfficientNet_V2_S_Weights,
        "summary": __import__("torchinfo", fromlist=["summary"]).summary,
        "tqdm": __import__("tqdm.auto", fromlist=["tqdm"]).tqdm,
    }


class MyType1Accelerator(RunnableComponent):
    """
    Restored CXL.cache-based accelerator with synchronous threading.
    """

    def __init__(
        self,
        port_index: int,
        host: str = "0.0.0.0",
        port: int = 8000,
        irq_port: int = 8500,
        device_id: int = 0,
        train_data_path: str = "",
    ):
        label = f"Port{port_index}"
        super().__init__(label)

        if not os.path.exists(train_data_path) or not os.path.isdir(train_data_path):
            raise Exception(f"Path {train_data_path} does not exist, or is not a folder.")

        self._sw_conn_client = SwitchConnectionClient(
            port_index, CXL_COMPONENT_TYPE.T1, host=host, port=port
        )
        self._cxl_type1_device = CxlType1Device(
            CxlType1DeviceConfig(
                transport_connection=self._sw_conn_client.get_cxl_connection(),
                device_name=label,
                device_id=device_id,
            )
        )
        self._device_id = device_id
        self.original_base_folder = train_data_path
        self.accel_dirname = f"/tmp/T1Accel@{self._label}"
        if os.path.exists(self.accel_dirname) and os.path.isdir(self.accel_dirname):
            shutil.rmtree(self.accel_dirname)
        Path(self.accel_dirname).mkdir(parents=True, exist_ok=True)
        self._train_folder = os.path.abspath(os.path.join(self.accel_dirname, "train"))
        self._val_folder = os.path.abspath(os.path.join(self.accel_dirname, "val"))
        symlink_train_src = os.path.abspath(os.path.join(self.original_base_folder, "train"))
        symlink_val_src = os.path.abspath(os.path.join(self.original_base_folder, "val"))
        os.symlink(src=symlink_train_src, dst=self._train_folder, target_is_directory=True)
        os.symlink(src=symlink_val_src, dst=self._val_folder, target_is_directory=True)

        # IRQ
        self._irq_manager = IrqManager(addr="127.0.0.1", port=irq_port, device_name=label, device_id=device_id)
        self._irq_manager.register_interrupt_handler(Irq.HOST_READY, self._run_app)
        self._irq_manager.register_interrupt_handler(Irq.HOST_SENT, self._validate_model)

        # Runtime state
        self._stop_event = threading.Event()
        self._stop_flag = False

        # ML members filled on setup
        self._ml = None
        self._model = None
        self._transform = None
        self._train_dataset = None
        self._train_dataloader = None
        self._test_dataset = None
        self._test_dataloader = None
        self._torch_device = None

    def set_stop_flag(self):
        self._stop_flag = True

    def _setup_model(self):
        if self._ml is None:
            self._ml = _lazy_import_ml()
        torch = self._ml["torch"]
        nn = self._ml["nn"]
        transforms = self._ml["transforms"]
        datasets = self._ml["datasets"]
        efficientnet_v2_s = self._ml["efficientnet_v2_s"]
        EfficientNet_V2_S_Weights = self._ml["EfficientNet_V2_S_Weights"]
        summary = self._ml["summary"]

        self._model = efficientnet_v2_s(weights=EfficientNet_V2_S_Weights.DEFAULT)
        self._model.classifier[1] = nn.Linear(in_features=1280, out_features=10, bias=True)
        for p in self._model.features.parameters():
            p.requires_grad = False
        summary(self._model, input_size=(1, 3, 160, 160))

        self._transform = transforms.Compose([transforms.Resize((160, 160)), transforms.ToTensor()])
        self._train_dataset = datasets.ImageFolder(root=self._train_folder, transform=self._transform)
        self._train_dataloader = self._ml["DataLoader"](
            self._train_dataset, batch_size=32, shuffle=True, num_workers=4
        )
        self._test_dataset = datasets.ImageFolder(root=self._val_folder, transform=self._transform)
        self._test_dataloader = self._ml["DataLoader"](
            self._train_dataset, batch_size=10, shuffle=True, num_workers=4
        )

    def _train_one_epoch(self):
        self._setup_model()
        torch = self._ml["torch"]
        tqdm = self._ml["tqdm"]
        device = self._torch_device
        loss_fn = torch.nn.CrossEntropyLoss()
        optimizer = torch.optim.SGD(self._model.parameters())
        self._model.train()
        correct_count = 0
        running_train_loss = 0
        for _, (inputs, labels) in tqdm(
            enumerate(self._train_dataloader),
            total=len(self._train_dataloader),
            desc=f"Dev {self._device_id} Training Progress",
            position=self._device_id,
            leave=False,
        ):
            if self._stop_flag:
                return
            inputs = inputs.to(device)
            labels = labels.to(device)
            pred_logits = self._model(inputs)
            loss = loss_fn(pred_logits, labels)
            predicted_prob = torch.softmax(pred_logits, dim=1)
            pred_classes = torch.argmax(predicted_prob, dim=1)
            loss.backward()
            optimizer.step()
            is_correct = pred_classes == labels
            correct_count += is_correct.sum()
            running_train_loss += loss.item() * inputs.size(0)
        train_loss = running_train_loss / len(self._train_dataloader.sampler)
        train_accuracy = correct_count / len(self._train_dataloader.sampler)
        logger.debug(self._create_message(f"train_loss: {train_loss}, train_accuracy: {train_accuracy}"))
        if str(device) == "cuda:0":
            torch.cuda.empty_cache()

    def _get_metadata(self):
        metadata_addr_mmio_addr = 0x1800
        metadata_size_mmio_addr = 0x1808
        metadata_addr = self._cxl_type1_device.read_mmio(metadata_addr_mmio_addr, 8)
        metadata_size = self._cxl_type1_device.read_mmio(metadata_size_mmio_addr, 8)
        metadata_rounded_size = ((metadata_size - 1) // 64 + 1) * 64
        metadata_end = metadata_addr + metadata_size
        out_path = f"{self.accel_dirname}{os.path.sep}noisy_imagenette.csv"
        with open(out_path, "wb") as md_file:
            start = metadata_addr
            end = metadata_addr + metadata_rounded_size
            for cacheline_offset in range(start, end, 64):
                cacheline = self._cxl_type1_device.cxl_cache_readline(cacheline_offset)
                chunk_size = min(64, (metadata_end - cacheline_offset))
                chunk_data = cacheline.to_bytes(64, "little")
                md_file.write(chunk_data[:chunk_size])
        logger.info(self._create_message(f"Dev {self._device_id} Finished writing file"))

    def _get_test_image(self):
        Image = __import__("PIL.Image", fromlist=["Image"]).Image
        image_addr_mmio_addr = 0x1810
        image_size_mmio_addr = 0x1818
        image_addr = self._cxl_type1_device.read_mmio(image_addr_mmio_addr, 8)
        image_size = self._cxl_type1_device.read_mmio(image_size_mmio_addr, 8)
        image_end = image_addr + image_size
        imgbuf = BytesIO()
        data = self._cxl_type1_device.cxl_cache_read(image_addr, image_size)
        imgbuf.write(data[: image_end - image_addr])
        imgbuf.seek(0)
        return Image.open(imgbuf).convert("RGB")

    def _validate_model(self, _dev_id):
        self._setup_model()
        torch = self._ml["torch"]
        im = self._get_test_image()
        tens = cast(torch.Tensor, self._transform(im))
        tens = torch.unsqueeze(tens, 0).to(self._torch_device)
        pred_logit = self._model(tens)
        predicted_probs = torch.softmax(pred_logit, dim=1)[0]
        categories = glob.glob(f"{self._val_folder}{os.path.sep}*")
        pred_kv = {self._test_dataset.classes[i]: predicted_probs[i].item() for i in range(len(categories))}
        json_asenc = str.encode(json.dumps(pred_kv))
        bytes_size = len(json_asenc)
        json_asint = int.from_bytes(json_asenc, "little")
        RESULTS_HPA = 0x900
        rounded_bytes_size = (((bytes_size - 1) // 64) + 1) * 64
        self._cxl_type1_device.cxl_cache_write(RESULTS_HPA, max(64, rounded_bytes_size), json_asint)
        HOST_VECTOR_ADDR = 0x1820
        HOST_VECTOR_SIZE = 0x1828
        self._cxl_type1_device.write_mmio(HOST_VECTOR_ADDR, 8, RESULTS_HPA)
        self._cxl_type1_device.write_mmio(HOST_VECTOR_SIZE, 8, bytes_size)
        self._irq_manager.send_irq_request(Irq.ACCEL_VALIDATION_FINISHED)

    def _run_app(self, _dev_id):
        self._ml = _lazy_import_ml()
        torch = self._ml["torch"]
        self._torch_device = torch.device("cuda:0") if torch.cuda.is_available() else torch.device("cpu")
        logger.debug(self._create_message(f"Using torch.device: {self._torch_device}"))
        self._setup_model()
        logger.info(self._create_message("Getting metadata for the image dataset"))
        self._get_metadata()
        logger.info(self._create_message("Begin Model Training"))
        self._train_one_epoch()
        logger.info(self._create_message("Done Model Training"))
        self._irq_manager.send_irq_request(Irq.ACCEL_TRAINING_FINISHED)

    def _run(self):
        self._stop_event.clear()
        self._sw_conn_client.start_wait_ready()
        self._cxl_type1_device.start_wait_ready()
        self._irq_manager.start_wait_ready()
        self._change_status_to_running()
        self._stop_event.wait()

    def _stop(self):
        self._stop_flag = True
        self._stop_event.set()
        try:
            self._irq_manager.stop_sync()
        except Exception:
            pass
        try:
            self._cxl_type1_device.stop_sync()
        except Exception:
            pass
        try:
            self._sw_conn_client.stop_sync()
        except Exception:
            pass
        try:
            shutil.rmtree(self.accel_dirname, ignore_errors=True)
        except Exception:
            pass


class MyType2Accelerator(RunnableComponent):
    """
    Restored CXL.mem-based accelerator with synchronous threading.
    """

    def __init__(
        self,
        port_index: int,
        memory_size: int,
        memory_file: str,
        host: str = "0.0.0.0",
        port: int = 8000,
        irq_port: int = 8500,
        device_id: int = 0,
        train_data_path: str | None = None,
    ):
        label = f"Port{port_index}"
        super().__init__(label)
        self._device_id = device_id
        self.accel_dirname = f"/tmp/T2Accel@{port_index}"
        self.train_data_path = train_data_path or ""
        self._torch_device = None
        self._ml = None

        self._sw_conn_client = SwitchConnectionClient(
            port_index, CXL_COMPONENT_TYPE.T2, host=host, port=port
        )
        self._device_config = CxlType2DeviceConfig(
            device_name=label,
            transport_connection=self._sw_conn_client.get_cxl_connection(),
            memory_size=memory_size,
            memory_file=memory_file,
        )
        self._cxl_type2_device: CxlType2Device | None = None

        self._irq_manager = IrqManager(
            addr="localhost",
            port=irq_port,
            device_name=label,
            device_id=device_id,
        )
        self._irq_manager.register_interrupt_handler(Irq.HOST_READY, self._run_app)
        self._irq_manager.register_interrupt_handler(Irq.HOST_SENT, self._validate_model)

        self._stop_event = threading.Event()
        self._transform = None
        self._train_dataset = None
        self._test_dataset = None
        self._train_dataloader = None
        self._test_dataloader = None
        self._val_folder = None
        self._model = None

    def _setup_test_env(self):
        if not os.path.isdir(self.accel_dirname):
            os.mkdir(self.accel_dirname)
        train_dir = os.path.abspath(os.path.join(self.train_data_path, "train"))
        val_dir = os.path.abspath(os.path.join(self.train_data_path, "val"))
        self._val_folder = val_dir
        os.chdir(self.accel_dirname)
        self._cxl_type2_device = CxlType2Device(self._device_config)
        self._ml = _lazy_import_ml()
        nn = self._ml["nn"]
        transforms = self._ml["transforms"]
        datasets = self._ml["datasets"]
        efficientnet_v2_s = self._ml["efficientnet_v2_s"]
        EfficientNet_V2_S_Weights = self._ml["EfficientNet_V2_S_Weights"]
        summary = self._ml["summary"]
        self._model = efficientnet_v2_s(weights=EfficientNet_V2_S_Weights.DEFAULT)
        self._model.classifier[1] = nn.Linear(in_features=1280, out_features=10, bias=True)
        for p in self._model.features.parameters():
            p.requires_grad = False
        summary(self._model, input_size=(1, 3, 160, 160))
        self._transform = transforms.Compose([transforms.Resize((160, 160)), transforms.ToTensor()])
        self._train_dataset = datasets.ImageFolder(root=train_dir, transform=self._transform)
        self._train_dataloader = self._ml["DataLoader"](
            self._train_dataset, batch_size=32, shuffle=True, num_workers=4
        )
        self._test_dataset = datasets.ImageFolder(root=val_dir, transform=self._transform)
        self._test_dataloader = self._ml["DataLoader"](
            self._train_dataset, batch_size=10, shuffle=True, num_workers=4
        )

    def _train_one_epoch(self):
        self._setup_test_env()
        torch = self._ml["torch"]
        tqdm = self._ml["tqdm"]
        device = self._torch_device
        loss_fn = torch.nn.CrossEntropyLoss()
        optimizer = torch.optim.SGD(self._model.parameters())
        self._model.train()
        for _, (inputs, labels) in tqdm(
            enumerate(self._train_dataloader),
            total=len(self._train_dataloader),
            desc=f"Dev {self._device_id} Training Progress",
            position=self._device_id,
            leave=False,
        ):
            inputs = inputs.to(device)
            labels = labels.to(device)
            pred_logits = self._model(inputs)
            loss = loss_fn(pred_logits, labels)
            predicted_prob = torch.softmax(pred_logits, dim=1)
            pred_classes = torch.argmax(predicted_prob, dim=1)
            loss.backward()
            optimizer.step()
        if str(device) == "cuda:0":
            torch.cuda.empty_cache()

    def _get_metadata(self):
        assert self._cxl_type2_device is not None
        metadata_addr_mmio_addr = 0x1800
        metadata_size_mmio_addr = 0x1808
        metadata_addr = self._cxl_type2_device.read_mmio(metadata_addr_mmio_addr, 8)
        metadata_size = self._cxl_type2_device.read_mmio(metadata_size_mmio_addr, 8)
        metadata_size = ((metadata_size - 1) // 64 + 1) * 64
        with open("noisy_imagenette.csv", "wb") as md_file:
            for offset in range(0, metadata_size, 64):
                data = self._cxl_type2_device.read_mem_dpa(metadata_addr + offset, 64)
                md_file.write(data.to_bytes(64, byteorder="little"))

    def _get_test_image(self):
        Image = __import__("PIL.Image", fromlist=["Image"]).Image
        assert self._cxl_type2_device is not None
        image_addr_mmio_addr = 0x1810
        image_size_mmio_addr = 0x1818
        image_addr = self._cxl_type2_device.read_mmio(image_addr_mmio_addr, 8)
        image_size = self._cxl_type2_device.read_mmio(image_size_mmio_addr, 8)
        end = image_addr + image_size
        with BytesIO() as imgbuf:
            for addr in range(image_addr, image_addr + image_size + 64, 64):
                data = self._cxl_type2_device.read_mem_dpa(addr, 64)
                chunk_size = min(64, (end - addr))
                chunk_data = data.to_bytes(64, "little")[:chunk_size]
                imgbuf.write(chunk_data)
            imgbuf.seek(0)
            return Image.open(imgbuf).convert("RGB")

    def _validate_model(self, _dev_id):
        self._setup_test_env()
        torch = self._ml["torch"]
        im = self._get_test_image()
        tens = cast(torch.Tensor, self._transform(im))
        tens = torch.unsqueeze(tens, 0).to(self._torch_device)
        pred_logit = self._model(tens)
        predicted_probs = torch.softmax(pred_logit, dim=1)[0]
        categories = glob.glob(f"{self._val_folder}{os.path.sep}*")
        pred_kv = {self._test_dataset.classes[i]: predicted_probs[i].item() for i in range(len(categories))}
        json_asenc = str.encode(json.dumps(pred_kv))
        bytes_size = len(json_asenc)
        json_asint = int.from_bytes(json_asenc, "little")
        RESULTS_OFFSET = 0x900
        rounded_bytes_size = math.ceil(bytes_size / 64) * 64
        curr_written = 0
        assert self._cxl_type2_device is not None
        while curr_written < rounded_bytes_size:
            chunk = json_asint & ((1 << (64 * 8)) - 1)
            self._cxl_type2_device.write_mem_dpa(RESULTS_OFFSET + curr_written, chunk, 64)
            json_asint >>= 64 * 8
            curr_written += 64
        HOST_VECTOR_ADDR = 0x1820
        HOST_VECTOR_SIZE = 0x1828
        self._cxl_type2_device.write_mmio(HOST_VECTOR_ADDR, 8, RESULTS_OFFSET)
        self._cxl_type2_device.write_mmio(HOST_VECTOR_SIZE, 8, bytes_size)
        self._irq_manager.send_irq_request(Irq.ACCEL_VALIDATION_FINISHED)

    def _run_app(self, _dev_id):
        self._ml = _lazy_import_ml()
        torch = self._ml["torch"]
        self._torch_device = torch.device("cuda:0") if torch.cuda.is_available() else torch.device("cpu")
        self._setup_test_env()
        logger.info(self._create_message("Getting metadata for the image dataset"))
        self._get_metadata()
        logger.info(self._create_message("Begin Model Training"))
        self._train_one_epoch()
        logger.info(self._create_message("Done Model Training"))
        self._irq_manager.send_irq_request(Irq.ACCEL_TRAINING_FINISHED)

    def _run(self):
        self._stop_event.clear()
        self._sw_conn_client.start_wait_ready()
        # Device starts after _setup_test_env when IRQ triggers; keep IRQ up
        self._irq_manager.start_wait_ready()
        self._change_status_to_running()
        self._stop_event.wait()

    def _stop(self):
        self._stop_event.set()
        try:
            self._irq_manager.stop_sync()
        except Exception:
            pass
        try:
            if self._cxl_type2_device is not None:
                self._cxl_type2_device.stop_sync()
        except Exception:
            pass
        try:
            self._sw_conn_client.stop_sync()
        except Exception:
            pass
        try:
            shutil.rmtree(self.accel_dirname, ignore_errors=True)
        except Exception:
            pass
