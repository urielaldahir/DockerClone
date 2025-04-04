import multiprocessing
import threading
import time
import random

# Diccionario global para almacenar los procesos trabajadores.
workers = {}

def worker_process(worker_id):
    """
    Función que ejecuta cada proceso trabajador.
    Simula la ejecución de tareas y genera una falla aleatoria.
    """
    while True:
        try:
            print(f"[Worker {worker_id}] Ejecutando tarea...")
            # Simula un tiempo de procesamiento variable
            time.sleep(random.uniform(0.5, 2.0))
            # Simula una falla aleatoria con un 20% de probabilidad
            if random.random() < 0.2:
                raise Exception("Fallo simulado")
        except Exception as e:
            print(f"[Worker {worker_id}] Error: {e}")
            break  # Sale del bucle al ocurrir una excepción
    print(f"[Worker {worker_id}] Finalizando proceso.")

def start_worker(worker_id):
    """
    Inicia un proceso trabajador y lo registra en el diccionario global.
    """
    p = multiprocessing.Process(target=worker_process, args=(worker_id,))
    p.start()
    workers[worker_id] = p

def monitor_workers():
    """
    Función que se ejecuta en un hilo daemon.
    Supervisa los procesos trabajadores y reinicia aquellos que hayan finalizado.
    """
    while True:
        time.sleep(2)
        for worker_id, process in list(workers.items()):
            if not process.is_alive():
                print(f"[Monitor] El Worker {worker_id} se cayó. Reiniciando...")
                start_worker(worker_id)

def main():
    num_workers = 3  # Número de procesos trabajadores iniciales

    # Inicia los procesos trabajadores
    for i in range(1, num_workers + 1):
        start_worker(i)

    # Inicia el hilo monitor como daemon (se ejecuta en segundo plano)
    monitor = threading.Thread(target=monitor_workers, daemon=True)
    monitor.start()

    try:
        # Mantiene el proceso principal en ejecución
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Main: Interrupción detectada. Terminando trabajadores...")
        # Termina cada proceso trabajador
        for process in workers.values():
            process.terminate()
        for process in workers.values():
            process.join()
        print("Main: Finalizando el programa.")

if __name__ == '__main__':
    main()
