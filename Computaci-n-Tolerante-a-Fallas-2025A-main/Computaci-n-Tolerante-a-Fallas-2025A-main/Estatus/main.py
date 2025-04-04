import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import time
import psutil
import threading

class AppMonitorService(win32serviceutil.ServiceFramework):
    _svc_name_ = "AppMonitorService"
    _svc_display_name_ = "Servicio de Monitor de Aplicación"
    _svc_description_ = "Supervisa el estado de un proceso específico (ej. notepad.exe)"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        # Crear un evento para esperar la señal de stop
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.is_running = True

    def SvcStop(self):
        # Registrar que se está parando el servicio
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        # Señalizar el evento para salir del bucle principal
        win32event.SetEvent(self.hWaitStop)
        self.is_running = False

    def SvcDoRun(self):
        # Registrar el inicio del servicio en el log de eventos
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def monitor_app(self):
        """
        Función que se ejecuta en un hilo separado para verificar si el proceso está activo.
        En este ejemplo se revisa si 'notepad.exe' está corriendo.
        """
        process_name = "notepad.exe"
        while self.is_running:
            # Iterar sobre los procesos en ejecución
            proceso_encontrado = any(proc.name().lower() == process_name for proc in psutil.process_iter())
            if proceso_encontrado:
                servicemanager.LogInfoMsg(f"El proceso {process_name} está corriendo.")
            else:
                servicemanager.LogInfoMsg(f"El proceso {process_name} NO está corriendo.")
            time.sleep(5)  # Espera de 5 segundos entre chequeos

    def main(self):
        # Crear y lanzar el hilo que monitorea el proceso
        monitor_thread = threading.Thread(target=self.monitor_app)
        monitor_thread.start()
        # Espera hasta que se reciba la señal para detener el servicio
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
        monitor_thread.join()

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(AppMonitorService)
