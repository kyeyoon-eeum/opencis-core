"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading
from typing import cast, Any, Tuple, Optional, Callable, Dict

from opencis.cxl.component.mctp.mctp_connection import MctpConnection
from opencis.cxl.transport.packet_constants import CCI_MCTP_MESSAGE_CATEGORY
from opencis.cxl.transport.cci_packets import CciMessagePacket, CciPayloadPacket
from opencis.cxl.cci.common import get_opcode_string
from opencis.cxl.cci.generic.information_and_status import (
    BackgroundOperationStatusCommand,
    BackgroundOperationStatusResponsePayload,
)
from opencis.cxl.cci.fabric_manager.physical_switch import (
    IdentifySwitchDeviceCommand,
    IdentifySwitchDeviceResponsePayload,
    GetPhysicalPortStateCommand,
    GetPhysicalPortStateRequestPayload,
    GetPhysicalPortStateResponsePayload,
)
from opencis.cxl.cci.fabric_manager.virtual_switch import (
    GetVirtualCxlSwitchInfoCommand,
    GetVirtualCxlSwitchInfoRequestPayload,
    GetVirtualCxlSwitchInfoResponsePayload,
    BindVppbCommand,
    BindVppbRequestPayload,
    UnbindVppbCommand,
    UnbindVppbRequestPayload,
    FreezeVppbCommand,
    FreezeVppbRequestPayload,
    UnfreezeVppbCommand,
    UnfreezeVppbRequestPayload,
)
from opencis.cxl.cci.fabric_manager.mld_components import (
    GetLdInfoCommand,
    GetLdInfoResponsePayload,
    GetLdAllocationsCommand,
    GetLdAllocationsRequestPayload,
    GetLdAllocationsResponsePayload,
    SetLdAllocationsCommand,
    SetLdAllocationsRequestPayload,
    SetLdAllocationsResponsePayload,
)
from opencis.cxl.cci.vendor_specfic import (
    GetConnectedDevicesCommand,
    GetConnectedDevicesResponsePayload,
)
from opencis.cxl.cci.common import CCI_RETURN_CODE
from opencis.cxl.component.cci_executor import CciRequest
from opencis.util.component import RunnableComponent
from opencis.util.logger import logger

CreateRequestFuncType = Callable[[Optional[Any]], CciRequest]
EventHandlerType = Callable[[CciMessagePacket], None]


class MctpCciApiClient(RunnableComponent):
    def __init__(self, mctp_connection: MctpConnection):
        super().__init__()
        self._mctp_connection = mctp_connection
        self._tag = 0
        self._responses: Dict[int, CciMessagePacket] = {}
        self._condition = threading.Condition()
        self._notification_handler = None
        self._in_thread: threading.Thread | None = None
        self._in_stop = threading.Event()

    def _process_incoming_packets(self):
        while True:
            raw_response = self._mctp_connection.ep_to_controller.get()
            if raw_response is None:
                break

            response_tmc1 = cast(CciPayloadPacket, raw_response)
            cci_message = response_tmc1.get_cci_message()
            if cci_message.cci_msg_header.message_category == CCI_MCTP_MESSAGE_CATEGORY.REQUEST:
                opcode_str = get_opcode_string(cci_message.cci_msg_header.command_opcode)
                logger.debug(
                    self._create_message(f"Received request (notification) packet {opcode_str}")
                )
                if self._notification_handler is not None:
                    logger.debug(
                        self._create_message(f"Calling handler for {opcode_str} notification")
                    )
                    self._notification_handler(cci_message)
            else:
                logger.debug(self._create_message("Received response packet"))
                with self._condition:
                    self._responses[cci_message.cci_msg_header.message_tag] = cci_message
                    self._condition.notify_all()

    def _run(self):
        # Start incoming worker thread
        self._in_stop.clear()
        self._in_thread = threading.Thread(
            target=self._incoming_worker,
            name=f"{self.get_message_label()}-in",
            daemon=True,
        )
        self._in_thread.start()
        self._change_status_to_running()
        # Keep component alive; thread will handle packets
        stopper = threading.Event()
        try:
            stopper.wait()
        except KeyboardInterrupt:
            pass

    def _stop(self):
        self._in_stop.set()
        try:
            self._mctp_connection.ep_to_controller.put(None)
        except Exception:
            pass
        if self._in_thread is not None:
            self._in_thread.join(timeout=1.0)

    def _get_response(self, message_tag: int) -> CciMessagePacket:
        with self._condition:
            logger.debug(self._create_message(f"Waiting for Message {message_tag}"))
            while message_tag not in self._responses:
                self._condition.wait()
            logger.debug(self._create_message(f"Received Message {message_tag}"))
            response = self._responses[message_tag]
            return response

    def _send_request(self, request: CciMessagePacket, port_index=0, _=0) -> CciMessagePacket:
        request.cci_msg_header.message_tag = self._get_next_tag()
        opcode_name = get_opcode_string(request.cci_msg_header.command_opcode)
        req_tag = request.cci_msg_header.message_tag
        logger.debug(self._create_message(f"Sending {opcode_name} (Tag: {req_tag})"))
        # wrapping
        request_tmc = CciPayloadPacket.create(request, port_index)

        self._mctp_connection.controller_to_ep.put(request_tmc)
        response = self._get_response(req_tag)
        res_tag = response.cci_msg_header.message_tag
        logger.debug(self._create_message(f"Received Response (Tag: {res_tag})"))

        if (
            response.cci_msg_header.background_operation
            and response.cci_msg_header.return_code == CCI_RETURN_CODE.BACKGROUND_COMMAND_STARTED
        ):
            logger.debug(self._create_message("Background Command Started"))
            return response

        if response.cci_msg_header.return_code != CCI_RETURN_CODE.SUCCESS:
            return_code_str = CCI_RETURN_CODE(response.cci_msg_header.return_code).name
            message = f"Command failed with status: {return_code_str}"
            logger.debug(self._create_message(message))

        return response

    def _get_next_tag(self) -> int:
        tag = self._tag
        self._tag += 1
        return tag

    def _create_request_packet(self, request: CciRequest) -> CciMessagePacket:
        message_packet = CciMessagePacket.create(
            data=request.payload,
            message_category=CCI_MCTP_MESSAGE_CATEGORY.REQUEST,
            opcode=request.opcode,
        )
        return message_packet

    def _wait_for_background_operation(self) -> CCI_RETURN_CODE:
        completed = False
        while not completed:
            (return_code, result) = self.background_operation_status()
            if not result:
                continue
            completed = not result.background_operation_status.operation_in_progress
            if completed:
                return return_code
        # TODO: Handle timeout

    def _send_cci_command(
        self, create_request_func: CreateRequestFuncType, request=None, port_index=0, ld_id=0
    ):
        cci_request = create_request_func() if request is None else create_request_func(request)
        request_message_packet = self._create_request_packet(cci_request)
        return self._send_request(request_message_packet, port_index, ld_id)

    def register_notification_handler(self, notification_handler: EventHandlerType):
        self._notification_handler = notification_handler

    def background_operation_status(
        self,
    ) -> Tuple[CCI_RETURN_CODE, Optional[BackgroundOperationStatusResponsePayload]]:
        response_message_packet = self._send_cci_command(
            BackgroundOperationStatusCommand.create_cci_request
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if return_code != CCI_RETURN_CODE.SUCCESS:
            return (return_code, None)
        response = BackgroundOperationStatusCommand.parse_response_payload(
            response_message_packet.get_payload()
        )
        # logger.debug(self._create_message(response.get_pretty_print()))
        return (return_code, response)

    def identify_switch_device(
        self,
    ) -> Tuple[CCI_RETURN_CODE, Optional[IdentifySwitchDeviceResponsePayload]]:
        response_message_packet = self._send_cci_command(
            IdentifySwitchDeviceCommand.create_cci_request
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if return_code != CCI_RETURN_CODE.SUCCESS:
            return (return_code, None)
        response = IdentifySwitchDeviceCommand.parse_response_payload(
            response_message_packet.get_payload()
        )
        logger.debug(self._create_message(response.get_pretty_print()))
        return (return_code, response)

    def get_physical_port_state(
        self, request: GetPhysicalPortStateRequestPayload
    ) -> Tuple[CCI_RETURN_CODE, Optional[GetPhysicalPortStateResponsePayload]]:
        response_message_packet = self._send_cci_command(
            GetPhysicalPortStateCommand.create_cci_request, request
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if return_code != CCI_RETURN_CODE.SUCCESS:
            return (return_code, None)
        response = GetPhysicalPortStateCommand.parse_response_payload(
            response_message_packet.get_payload()
        )
        # logger.debug(self._create_message(response.get_pretty_print()))
        return (return_code, response)

    def get_virtual_cxl_switch_info(
        self, request: GetVirtualCxlSwitchInfoRequestPayload
    ) -> Tuple[CCI_RETURN_CODE, Optional[GetVirtualCxlSwitchInfoResponsePayload]]:
        response_message_packet = self._send_cci_command(
            GetVirtualCxlSwitchInfoCommand.create_cci_request, request
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if return_code != CCI_RETURN_CODE.SUCCESS:
            return (return_code, None)
        response = GetVirtualCxlSwitchInfoCommand.parse_response_payload(
            response_message_packet.get_payload(),
            request.start_vppb,
            request.vppb_list_limit,
        )
        logger.debug(self._create_message(response.get_pretty_print()))
        return (return_code, response)

    def bind_vppb(
        self, request: BindVppbRequestPayload, wait_for_completion: bool = True
    ) -> Tuple[CCI_RETURN_CODE, Optional[CCI_RETURN_CODE]]:
        response_message_packet = self._send_cci_command(
            BindVppbCommand.create_cci_request, request
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if wait_for_completion:
            return_code = self._wait_for_background_operation()
        if return_code not in (
            CCI_RETURN_CODE.SUCCESS,
            CCI_RETURN_CODE.BACKGROUND_COMMAND_STARTED,
        ):
            return (return_code, None)
        return (return_code, return_code)

    def unbind_vppb(
        self, request: UnbindVppbRequestPayload, wait_for_completion: bool = True
    ) -> Tuple[CCI_RETURN_CODE, Optional[CCI_RETURN_CODE]]:
        response_message_packet = self._send_cci_command(
            UnbindVppbCommand.create_cci_request, request
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if wait_for_completion:
            return_code = self._wait_for_background_operation()
        if return_code not in (
            CCI_RETURN_CODE.SUCCESS,
            CCI_RETURN_CODE.BACKGROUND_COMMAND_STARTED,
        ):
            return (return_code, None)
        return (return_code, return_code)

    def get_connected_devices(
        self,
    ) -> Tuple[CCI_RETURN_CODE, Optional[GetConnectedDevicesResponsePayload]]:
        response_message_packet = self._send_cci_command(
            GetConnectedDevicesCommand.create_cci_request
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if return_code != CCI_RETURN_CODE.SUCCESS:
            return (return_code, None)
        response = GetConnectedDevicesCommand.parse_response_payload(
            response_message_packet.get_payload()
        )
        return (return_code, response)

    def get_ld_info(
        self, port_index: int
    ) -> Tuple[CCI_RETURN_CODE, Optional[GetLdInfoResponsePayload]]:
        response_message_packet = self._send_cci_command(
            GetLdInfoCommand.create_cci_request, port_index=port_index
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if return_code != CCI_RETURN_CODE.SUCCESS:
            return (return_code, None)
        response = GetLdInfoCommand.parse_response_payload(response_message_packet.get_payload())
        return (return_code, response)

    def get_ld_alloctaion(
        self, request: GetLdAllocationsRequestPayload, port_index: int
    ) -> Tuple[CCI_RETURN_CODE, Optional[GetLdAllocationsResponsePayload]]:
        response_message_packet = self._send_cci_command(
            GetLdAllocationsCommand.create_cci_request, request, port_index=port_index
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if return_code != CCI_RETURN_CODE.SUCCESS:
            return (return_code, None)
        response = GetLdAllocationsCommand.parse_response_payload(
            response_message_packet.get_payload()
        )
        return (return_code, response)

    def set_ld_alloctaion(
        self, request: SetLdAllocationsRequestPayload, port_index: int
    ) -> Tuple[CCI_RETURN_CODE, Optional[SetLdAllocationsResponsePayload]]:
        response_message_packet = self._send_cci_command(
            SetLdAllocationsCommand.create_cci_request, request, port_index=port_index
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if return_code != CCI_RETURN_CODE.SUCCESS:
            return (return_code, None)
        response = SetLdAllocationsCommand.parse_response_payload(
            response_message_packet.get_payload()
        )
        return (return_code, response)

    def freeze_vppb(
        self, request: FreezeVppbRequestPayload, wait_for_completion: bool = True
    ) -> Tuple[CCI_RETURN_CODE, Optional[CCI_RETURN_CODE]]:
        response_message_packet = self._send_cci_command(
            FreezeVppbCommand.create_cci_request, request
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if wait_for_completion:
            return_code = self._wait_for_background_operation()
        if return_code not in (
            CCI_RETURN_CODE.SUCCESS,
            CCI_RETURN_CODE.BACKGROUND_COMMAND_STARTED,
        ):
            return (return_code, None)
        return (return_code, return_code)

    def unfreeze_vppb(
        self, request: UnfreezeVppbRequestPayload, wait_for_completion: bool = True
    ) -> Tuple[CCI_RETURN_CODE, Optional[CCI_RETURN_CODE]]:
        response_message_packet = self._send_cci_command(
            UnfreezeVppbCommand.create_cci_request, request
        )

        return_code = CCI_RETURN_CODE(response_message_packet.cci_msg_header.return_code)
        if wait_for_completion:
            return_code = self._wait_for_background_operation()
        if return_code not in (
            CCI_RETURN_CODE.SUCCESS,
            CCI_RETURN_CODE.BACKGROUND_COMMAND_STARTED,
        ):
            return (return_code, None)
        return (return_code, return_code)

    def _incoming_worker(self) -> None:
        while not self._in_stop.is_set():
            packet = self._mctp_connection.ep_to_controller.get()
            if packet is None:
                break
            response_tmc1 = cast(CciPayloadPacket, packet)
            cci_message = response_tmc1.get_cci_message()
            if cci_message.cci_msg_header.message_category == CCI_MCTP_MESSAGE_CATEGORY.REQUEST:
                opcode_str = get_opcode_string(cci_message.cci_msg_header.command_opcode)
                logger.debug(
                    self._create_message(f"Received request (notification) packet {opcode_str}")
                )
                if self._notification_handler is not None:
                    self._notification_handler(cci_message)
            else:
                logger.debug(self._create_message("Received response packet"))

                # Use condition in the same thread context
                with self._condition:
                    self._responses[cci_message.cci_msg_header.message_tag] = cci_message
                    self._condition.notify_all()
