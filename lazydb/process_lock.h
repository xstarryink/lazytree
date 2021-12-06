/*
2020-1-11 wangxin
进程锁
*/
#ifndef __process_lock_h__
#define __process_lock_h__

#include <Windows.h>

class file_lock
{

public:

	enum class type
	{
		null = 0,
		read,
		write,
	};

	file_lock()
		:m_hFile(NULL)
		, m_type(type::null)
		, m_offset(0)
		, m_size(0)
        , m_locked(false)
	{

	}

	~file_lock()
	{
        unlock();
	}

	bool lock(HANDLE h, type t,unsigned long o, unsigned long s)
	{
		if (h && s && h != INVALID_HANDLE_VALUE && t != type::null && m_type == type::null)
		{
			switch (t)
			{
			case file_lock::type::read:
			{
				OVERLAPPED overlapped = { 0 };
				overlapped.Offset = o;
				m_hFile = h;
				m_type = t;
				m_offset = o;
				m_size = s;
				//只防止写
				if (::LockFileEx(h, 0, 0, s, 0, &overlapped))
				{
                    m_locked = true;
					return true;
				}
			}
			break;
			case file_lock::type::write:
			{
				OVERLAPPED overlapped = { 0 };
				overlapped.Offset = o;
				m_hFile = h;
				m_type = t;
				m_offset = o;
				m_size = s;
				//防止读写
				if(::LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK, 0, s, 0, &overlapped))
                {
                    m_locked = true;
					return true;
				}
			}
			break;
			default:
				break;
			}
		}
		return false;
	}

	bool unlock()
	{
        if (m_locked)
        {
            OVERLAPPED overlapped = { 0 };
            overlapped.Offset = m_offset;
            if (::UnlockFileEx(m_hFile, 0, m_size, 0, &overlapped))
            {
                m_hFile = NULL;
                m_type = type::null;
                m_offset = 0;
                m_size = 0;
                m_locked = false;
                return true;
            }
        }
		return false;
	}

private:

	HANDLE m_hFile;

	type m_type;

	unsigned long m_offset;

	unsigned long m_size;

    bool m_locked;

};

#endif //__process_lock_h__
