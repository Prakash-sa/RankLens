import { ImageResponse } from 'next/og';

export const size = { width: 64, height: 64 };
export const contentType = 'image/png';

export default function Icon() {
  return new ImageResponse(
    <div style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'flex-end', justifyContent: 'center', gap: 5, padding: 13, background: '#b8f35a' }}>
      <span style={{ width: 8, height: 17, background: '#080d09' }} />
      <span style={{ width: 8, height: 34, background: '#080d09' }} />
      <span style={{ width: 8, height: 25, background: '#080d09' }} />
    </div>,
    size,
  );
}
