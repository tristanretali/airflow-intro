# Airflow Intro

## Partie 5 : casser un DAG, lire l'échec, corriger

| Cas | Modification | Job qui échoue | Message à retrouver |
|---|---|---|---|
| A | Supprimer la ligne qui importe `PostgresHook` | `lint` | `F821 undefined name 'PostgresHook'` |
| B | Supprimer l'appel final `user_processing()` | `test-dags` | `assert 'user_processing' in dag_bag.dags` |
| C | Renommer la tâche `store_user` en `save_user` | `test-dags` | `FAILED tests/test_dags.py::test_user_processing_taches - AssertionError: assert {'create_tabl..., 'save_user'} == {'create_tabl... 'store_user'}` |
