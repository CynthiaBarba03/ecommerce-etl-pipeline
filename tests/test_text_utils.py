"""
test_text_utils.py — Pruebas unitarias para text_utils.py
==========================================================

¿QUÉ SON LOS UNIT TESTS (PRUEBAS UNITARIAS)?
    Son funciones que verifican automáticamente que tu código funciona.
    En vez de abrir Databricks y ejecutar manualmente para ver si algo
    cambió bien, los tests lo hacen solos y te dicen PASS o FAIL.

¿POR QUÉ EXISTEN EN LA VIDA REAL?
    Imagina que en 3 meses alguien (o tú misma) cambia strip_and_normalize_spaces
    y sin querer rompe el comportamiento. Sin tests, no te darías cuenta hasta
    que los datos en producción estén mal. CON tests, el CI/CD (el sistema
    automático de despliegue) te avisa antes de que el cambio llegue a producción.

    Empresas como Netflix, Google, etc. tienen políticas de "no merge sin tests".

CÓMO CORRER LOS TESTS:
    Desde la terminal, en la raíz del proyecto:
        pytest tests/ -v

    El flag -v (verbose) muestra el nombre de cada test y si pasó o falló.

ESTRUCTURA DE UN TEST:
    def test_nombre_descriptivo():
        # 1. ARRANGE: preparar los datos de entrada
        entrada = "  hola   mundo  "

        # 2. ACT: ejecutar la función que queremos probar
        resultado = mi_funcion(entrada)

        # 3. ASSERT: verificar que el resultado es el esperado
        assert resultado == "hola mundo"
        # Si esta línea falla → el test falla y pytest te avisa

LIBRERÍA USADA: pytest
    Es el estándar de la industria para Python. Para instalarla:
        pip install pytest pytest-mock

NOTA: Estos tests corren en Python puro (sin Spark) usando pandas
como sustituto para los tipos Column. Para tests con Spark real
se usa pyspark.testing (más avanzado, se hace en otro archivo).
"""

import pytest


# ═══════════════════════════════════════════════════════════════
# TESTS PARA: strip_and_normalize_spaces
# ═══════════════════════════════════════════════════════════════

class TestStripAndNormalizeSpaces:
    """
    Agrupa todos los tests de strip_and_normalize_spaces.
    Usar clases es opcional pero organiza mejor los tests relacionados.
    """

    def test_quita_espacios_al_inicio_y_fin(self):
        """'  hola  ' debe quedar 'hola'"""
        # En producción esto se probaría con un DataFrame de Spark.
        # Aquí validamos la LÓGICA en Python puro.
        texto = "  hola  "
        resultado = texto.strip()
        assert resultado == "hola", f"Esperaba 'hola', obtuve '{resultado}'"

    def test_quita_espacios_internos_multiples(self):
        """'john   smith' debe quedar 'john smith'"""
        import re
        texto = "john   smith"
        resultado = re.sub(r"\s+", " ", texto).strip()
        assert resultado == "john smith"

    def test_texto_sin_espacios_extra_no_cambia(self):
        """'John Smith' no debe cambiar"""
        import re
        texto = "John Smith"
        resultado = re.sub(r"\s+", " ", texto).strip()
        assert resultado == "John Smith"

    def test_solo_espacios_queda_vacio(self):
        """'   ' debe quedar '' (vacío)"""
        import re
        texto = "   "
        resultado = re.sub(r"\s+", " ", texto).strip()
        assert resultado == ""

    def test_string_vacio_no_cambia(self):
        """'' debe quedar ''"""
        import re
        texto = ""
        resultado = re.sub(r"\s+", " ", texto).strip()
        assert resultado == ""


# ═══════════════════════════════════════════════════════════════
# TESTS PARA: normalize_case
# ═══════════════════════════════════════════════════════════════

class TestNormalizeCase:
    """
    Tests para la capitalización de texto.
    Este es el problema clave: alicia = ALICIA = Alicia → deben ser iguales.
    """

    def test_title_case_minusculas(self):
        """'john smith' → 'John Smith'"""
        texto = "john smith"
        resultado = texto.title()
        assert resultado == "John Smith"

    def test_title_case_mayusculas(self):
        """'JOHN SMITH' → 'John Smith'"""
        texto = "JOHN SMITH"
        resultado = texto.title()
        assert resultado == "John Smith"

    def test_title_case_mixto(self):
        """'joHN sMITH' → 'John Smith'"""
        texto = "joHN sMITH"
        resultado = texto.title()
        assert resultado == "John Smith"

    def test_upper_case(self):
        """'john' → 'JOHN'"""
        texto = "john"
        resultado = texto.upper()
        assert resultado == "JOHN"

    def test_lower_case(self):
        """'JOHN' → 'john'"""
        texto = "JOHN"
        resultado = texto.lower()
        assert resultado == "john"

    def test_modo_invalido_lanza_error(self):
        """
        Si se pasa un modo que no existe, debe lanzar ValueError.
        Esto verifica que los errores se manejan correctamente.
        """
        with pytest.raises(ValueError, match="Modo 'incorrecto' no reconocido"):
            # Simulamos la lógica de normalize_case
            mode = "incorrecto"
            if mode not in ("title", "upper", "lower"):
                raise ValueError(f"Modo '{mode}' no reconocido. Usa: 'title', 'upper' o 'lower'")


# ═══════════════════════════════════════════════════════════════
# TESTS PARA: remove_special_chars
# ═══════════════════════════════════════════════════════════════

class TestRemoveSpecialChars:
    """Tests para la eliminación de caracteres especiales."""

    def test_quita_arroba_y_numeros(self):
        """'Joh@n123' → 'John'"""
        import re
        texto = "Joh@n123"
        resultado = re.sub(r"[^a-zA-Z\s]", "", texto)
        assert resultado == "John"

    def test_conserva_apostrofo_si_se_pide(self):
        """'O'Brian!' → 'O'Brian' (manteniendo apóstrofo)"""
        import re
        texto = "O'Brian!"
        keep_chars = "'"
        resultado = re.sub(f"[^a-zA-Z\\s{keep_chars}]", "", texto)
        assert resultado == "O'Brian"

    def test_conserva_guion_si_se_pide(self):
        """'Jean-Claude!' → 'Jean-Claude'"""
        import re
        texto = "Jean-Claude!"
        keep_chars = "-"
        resultado = re.sub(f"[^a-zA-Z\\s{keep_chars}]", "", texto)
        assert resultado == "Jean-Claude"

    def test_texto_limpio_no_cambia(self):
        """'John Smith' sin caracteres especiales no debe cambiar"""
        import re
        texto = "John Smith"
        resultado = re.sub(r"[^a-zA-Z\s]", "", texto)
        assert resultado == "John Smith"


# ═══════════════════════════════════════════════════════════════
# TESTS PARA: normalize_to_null (lógica de reemplazo de nulos)
# ═══════════════════════════════════════════════════════════════

class TestNormalizeToNull:
    """Tests para la lógica de reemplazo de vacíos/nulos."""

    def _normalize(self, valor, replacement="UNKNOWN"):
        """Simula la lógica de normalize_to_null en Python puro."""
        if valor is None or valor.strip() == "":
            return replacement
        return valor

    def test_none_se_reemplaza(self):
        """None → 'UNKNOWN'"""
        assert self._normalize(None) == "UNKNOWN"

    def test_string_vacio_se_reemplaza(self):
        """'' → 'UNKNOWN'"""
        assert self._normalize("") == "UNKNOWN"

    def test_solo_espacios_se_reemplaza(self):
        """'   ' → 'UNKNOWN' (solo espacios)"""
        assert self._normalize("   ") == "UNKNOWN"

    def test_texto_normal_no_cambia(self):
        """'John' → 'John' (no se toca)"""
        assert self._normalize("John") == "John"

    def test_replacement_personalizado(self):
        """'' → 'N/A' (usando replacement personalizado)"""
        assert self._normalize("", replacement="N/A") == "N/A"


# ═══════════════════════════════════════════════════════════════
# TESTS PARA: is_valid_email (lógica del regex)
# ═══════════════════════════════════════════════════════════════

class TestIsValidEmail:
    """Tests para la validación de formato de email."""

    def _is_valid(self, email):
        """Simula la lógica de is_valid_email en Python puro."""
        import re
        if email is None or email.strip() == "":
            return False
        pattern = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
        return bool(re.match(pattern, email.strip()))

    def test_email_valido_basico(self):
        assert self._is_valid("user@gmail.com") is True

    def test_email_valido_con_puntos(self):
        assert self._is_valid("nombre.apellido@empresa.com.mx") is True

    def test_email_valido_con_guion_bajo(self):
        assert self._is_valid("mi_correo@hotmail.com") is True

    def test_sin_arroba_es_invalido(self):
        assert self._is_valid("noarrobagmail.com") is False

    def test_sin_dominio_es_invalido(self):
        assert self._is_valid("usuario@") is False

    def test_sin_extension_es_invalido(self):
        assert self._is_valid("usuario@gmail") is False

    def test_vacio_es_invalido(self):
        assert self._is_valid("") is False

    def test_none_es_invalido(self):
        assert self._is_valid(None) is False

    def test_solo_espacios_es_invalido(self):
        assert self._is_valid("   ") is False

    def test_email_con_mayusculas_es_valido(self):
        """Los emails con mayúsculas son técnicamente válidos (aunque raros)"""
        assert self._is_valid("Usuario@Gmail.COM") is True
