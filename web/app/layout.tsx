import type { Metadata } from 'next';
import { Geist, Geist_Mono } from 'next/font/google';
import './globals.css';

const geistSans = Geist({ variable: '--font-geist-sans', subsets: ['latin'] });
const geistMono = Geist_Mono({ variable: '--font-geist-mono', subsets: ['latin'] });

export const metadata: Metadata = {
  title: 'RankLens — MPI Performance Intelligence',
  description: 'Explore RankLens MPI performance analysis, identify rank imbalance, and turn telemetry into testable optimization experiments.',
  openGraph: {
    title: 'RankLens — Find the rank holding everyone back',
    description: 'Evidence-driven MPI performance analysis for engineers running distributed workloads.',
    type: 'website',
    images: ['https://raw.githubusercontent.com/Prakash-sa/RankLens/main/web/public/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'RankLens — MPI Performance Intelligence',
    description: 'Find imbalance, collective pressure, and communication hotspots in MPI workloads.',
    images: ['https://raw.githubusercontent.com/Prakash-sa/RankLens/main/web/public/og.png'],
  },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="en"><body className={`${geistSans.variable} ${geistMono.variable}`}>{children}</body></html>;
}
