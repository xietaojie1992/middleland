package com.middleland.commons.common.base;

/**
 * @author xietaojie
 */
public class Asserts {

    /************ NoRollbackException asserts ************/

    public static void assertNotNull(Object obj, ICodeEnum e) throws NoRollbackException {
        assertTrue(obj != null, e.code(), e.message());
    }

    public static void assertNotNull(Object obj, ICodeEnum e, String msg) throws NoRollbackException {
        assertTrue(obj != null, e.code(), msg);
    }

    public static void assertNotNull(Object obj, String code, String msg) throws NoRollbackException {
        assertTrue(obj != null, code, msg);
    }

    public static void assertNull(Object object, ICodeEnum e, String msg) throws NoRollbackException {
        assertTrue(object == null, e.code(), msg);
    }

    public static void assertNull(Object object, ICodeEnum e) throws NoRollbackException {
        assertTrue(object == null, e.code(), e.message());
    }

    public static void assertNull(Object object, String code, String msg) throws NoRollbackException {
        assertTrue(object == null, code, msg);
    }

    public static void assertTrue(boolean b, ICodeEnum e, String msg) throws NoRollbackException {
        assertTrue(b, e.code(), msg);
    }

    public static void assertTrue(boolean b, ICodeEnum e) throws NoRollbackException {
        assertTrue(b, e.code(), e.message());
    }

    public static void assertFalse(boolean b, ICodeEnum e, String msg) throws NoRollbackException {
        assertTrue(!b, e.code(), msg);
    }

    public static void assertFalse(boolean b, ICodeEnum e) throws NoRollbackException {
        assertTrue(!b, e.code(), e.message());
    }

    /************ RollbackException asserts ************/

    public static void assertNotNullOrRollback(Object obj, ICodeEnum e) throws RollbackException {
        assertTrueOrRollback(obj != null, e.code(), e.message());
    }

    public static void assertNotNullOrRollback(Object obj, ICodeEnum e, String msg) throws RollbackException {
        assertTrueOrRollback(obj != null, e.code(), msg);
    }

    public static void assertNotNullOrRollback(Object obj, String code, String msg) throws RollbackException {
        assertTrueOrRollback(obj != null, code, msg);
    }

    public static void assertNullOrRollback(Object object, ICodeEnum e, String msg) throws RollbackException {
        assertTrueOrRollback(object == null, e.code(), msg);
    }

    public static void assertNullOrRollback(Object object, ICodeEnum e) throws RollbackException {
        assertTrueOrRollback(object == null, e.code(), e.message());
    }

    public static void assertNullOrRollback(Object object, String code, String msg) throws RollbackException {
        assertTrueOrRollback(object == null, code, msg);
    }

    public static void assertTrueOrRollback(boolean b, ICodeEnum e, String msg) throws RollbackException {
        assertTrueOrRollback(b, e.code(), msg);
    }

    public static void assertTrueOrRollback(boolean b, ICodeEnum e) throws RollbackException {
        assertTrueOrRollback(b, e.code(), e.message());
    }

    public static void assertFalseOrRollback(boolean b, ICodeEnum e, String msg) throws RollbackException {
        assertTrueOrRollback(!b, e.code(), msg);
    }

    public static void assertFalseOrRollback(boolean b, ICodeEnum e) throws RollbackException {
        assertTrueOrRollback(!b, e.code(), e.message());
    }

    /************ Basic asserts ************/

    public static void assertTrue(boolean b, String code, String msg) throws NoRollbackException {
        if (!b) {
            throw new NoRollbackException(code, msg);
        }
    }

    public static void assertTrueOrRollback(boolean b, String code, String msg) throws RollbackException {
        if (!b) {
            throw new RollbackException(code, msg);
        }
    }

    public static void assertTrue(Result result) throws NoRollbackException {
        if (!result.isSuccess()) {
            throw new NoRollbackException(result.getCode(), result.getMessage());
        }
    }

    public static void assertTrueOrRollback(Result result) throws RollbackException {
        if (!result.isSuccess()) {
            throw new RollbackException(result.getCode(), result.getMessage());
        }
    }
}
