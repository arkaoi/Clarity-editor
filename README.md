# Clarity

> Понимайте и экспериментируйте с LSM-деревом, создавайте надежное key-value хранилище!

**Авторы**: [Заочинский Владимир](https://github.com/frmb3hind) и [Ким Андрей](https://github.com/arkaoi)

## Описание

**Clarity** — кроссплатформенное key-value хранилище на базе LSM-дерева, реализованное на C++ с использованием фреймворка [userver](https://github.com/userver-framework/userver). Обеспечивает быстрый доступ к JSON-данным по строковым ключам, гарантирует сохранность через WAL и оптимизированную работу дисковых сегментов (SSTable).

## Архитектура

1. **Memtable** на основе `SkipListMap` — горячее хранение в памяти.
2. **WAL** (write-ahead log) — журнал операций для восстановления после сбоев.
3. **SSTable** — устойчивые на диске файлы с Bloom-фильтром и индексом ключей.
4. **HTTP API** — CRUD-эндпоинты и генерация CSV-снэпшота.

## Функциональности

- `PUT /database/{key}` — вставка или обновление JSON-значения.
- `GET /database/{key}` — чтение значения по ключу.
- `DELETE /database/{key}` — удаление значения.
- `GET /snapshot` — CSV-дамп всех актуальных данных.
- Фоновые flush и merge SSTable, безопасная многопоточность.

## Используемые технологии

- C++17
- Userver (Yandex)
- CMake 3.14+
- SkipList, Bloom Filter, LSM-tree

## Установка и запуск

```bash
git clone --recursive https://github.com/arkaoi/Clarity-editor.git
cd Clarity
mkdir build && cd build
cmake ..
make -j
./clarity --config ../configs/config.yaml
```

Проверьте через `curl http://localhost:8080/ping`.

## Тестирование

Тесты, покрывающие все возможные сценарии работы, лежат в папке `tests/`. При желании их можно запустить самостоятельно:

```bash
python3 tests/test_load_and_snapshot.py
python3 tests/validate_snapshot_csv.py
python3 tests/multithread_test.py
```
